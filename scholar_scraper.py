import os
from dotenv import load_dotenv
from serpapi import GoogleSearch
import pandas as pd
from datetime import datetime
import time
import sys


load_dotenv()


class GoogleScholarScraperSerpAPI:
    def __init__(self, api_key):
        if not api_key:
            raise ValueError("API key is required")
        self.api_key = api_key
        self.results = []
        self.max_retries = 3
        self.retry_delay = 5  # seconds
    
    def handle_api_error(self, results, page_num):
        """
        Handle various API errors and return appropriate messages
        
        Args:
            results: API response dictionary
            page_num: Current page number
            
        Returns:
            tuple: (continue_scraping: bool, error_message: str)
        """
        # Check for HTTP-level errors
        if "error" in results:
            error_msg = results.get("error", "Unknown error")
            
            # Rate limit exceeded (API limit reached)
            if "rate limit" in error_msg.lower() or "too many requests" in error_msg.lower():
                print(f"\n{'='*50}")
                print("API RATE LIMIT EXCEEDED")
                print(f"{'='*50}")
                print(f"Error: {error_msg}")
                print("\nPossible reasons:")
                print("1. You've exceeded your hourly request limit")
                print("2. Your account has run out of searches")
                print("3. Too many requests in short time period")
                print("\nRecommendations:")
                print("- Check your SerpAPI dashboard for remaining credits")
                print("- Wait a few minutes before retrying")
                print("- Reduce the number of pages to scrape")
                print(f"{'='*50}\n")
                return False, "rate_limit"
            
            # Blocked/Forbidden access
            elif "forbidden" in error_msg.lower() or "unauthorized" in error_msg.lower():
                print(f"\n{'='*50}")
                print("ACCESS DENIED")
                print(f"{'='*50}")
                print(f"Error: {error_msg}")
                print("\nPossible reasons:")
                print("1. Invalid API key")
                print("2. Account has been suspended")
                print("3. Permission denied for this resource")
                print("\nRecommendations:")
                print("- Verify your API key in .env file")
                print("- Check account status on SerpAPI dashboard")
                print(f"{'='*50}\n")
                return False, "access_denied"
            
            # CAPTCHA or blocking issues
            elif "captcha" in error_msg.lower() or "blocked" in error_msg.lower():
                print(f"\nPage {page_num}: CAPTCHA/Blocking detected - {error_msg}")
                print("Note: SerpAPI usually handles CAPTCHAs automatically.")
                print("This might indicate a temporary issue.")
                return True, "captcha"  # Continue with next page
            
            # Empty results
            elif "no results" in error_msg.lower() or "hasn't returned" in error_msg.lower():
                print(f"\nPage {page_num}: No results found for this query")
                return False, "no_results"
            
            # Generic error
            else:
                print(f"\nPage {page_num}: Error - {error_msg}")
                return True, "generic_error"
        
        # Check search status
        search_metadata = results.get("search_metadata", {})
        status = search_metadata.get("status", "Unknown")
        
        if status == "Error":
            print(f"\nPage {page_num}: Search processing error")
            return True, "processing_error"
        
        # Check for empty results state
        search_info = results.get("search_information", {})
        results_state = search_info.get("organic_results_state", "")
        
        if "empty" in results_state.lower():
            print(f"\nPage {page_num}: No organic results found")
            return False, "empty_results"
        
        return True, None
    
    def safe_get_field(self, data, keys, default='N/A'):
        """
        Safely extract nested dictionary values
        
        Args:
            data: Dictionary to search
            keys: List of keys or single key
            default: Default value if not found
            
        Returns:
            Value or default
        """
        try:
            if isinstance(keys, list):
                value = data
                for key in keys:
                    value = value.get(key, {})
                return value if value else default
            else:
                return data.get(keys, default)
        except (AttributeError, TypeError, KeyError):
            return default
    
    def search_articles(self, query, num_pages=3, delay=1):
        """
        Search Google Scholar articles with pagination and error handling
        
        Args:
            query: Search query string (case-insensitive)
            num_pages: Number of pages to scrape
            delay: Delay between requests in seconds
        """
        self.results = []
        
        # Normalize query
        query = query.strip()
        
        print(f"\n{'='*50}")
        print(f"Searching for: '{query}'")
        print(f"Pages to fetch: {num_pages}")
        print(f"{'='*50}\n")
        
        consecutive_errors = 0
        max_consecutive_errors = 3
        
        for page in range(num_pages):
            retry_count = 0
            page_success = False
            
            while retry_count < self.max_retries and not page_success:
                try:
                    start = page * 10
                    
                    params = {
                        "engine": "google_scholar",
                        "q": query,
                        "api_key": self.api_key,
                        "start": start,
                        "num": 10
                    }
                    
                    print(f"Fetching page {page + 1}/{num_pages}...", end=" ")
                    search = GoogleSearch(params)
                    results = search.get_dict()
                    
                    # Handle API errors
                    continue_scraping, error_type = self.handle_api_error(results, page + 1)
                    
                    if not continue_scraping:
                        print(f"\nStopping scraper due to: {error_type}")
                        return pd.DataFrame(self.results) if self.results else pd.DataFrame()
                    
                    if error_type:
                        consecutive_errors += 1
                        if consecutive_errors >= max_consecutive_errors:
                            print(f"\nToo many consecutive errors ({consecutive_errors}). Stopping.")
                            return pd.DataFrame(self.results) if self.results else pd.DataFrame()
                        
                        retry_count += 1
                        if retry_count < self.max_retries:
                            wait_time = self.retry_delay * retry_count
                            print(f"Retrying in {wait_time} seconds... (Attempt {retry_count + 1}/{self.max_retries})")
                            time.sleep(wait_time)
                        continue
                    
                    # Reset consecutive errors on success
                    consecutive_errors = 0
                    
                    # Process organic results
                    if "organic_results" in results and results["organic_results"]:
                        for article in results["organic_results"]:
                            # Safe extraction of fields with fallbacks
                            title = self.safe_get_field(article, 'title', 'Untitled')
                            
                            # Handle missing or malformed publication info
                            pub_info = article.get('publication_info', {})
                            authors_list = pub_info.get('authors', [])
                            
                            if isinstance(authors_list, list) and authors_list:
                                authors = ', '.join([
                                    a.get('name', 'Unknown') if isinstance(a, dict) else str(a)
                                    for a in authors_list
                                ])
                            else:
                                authors = 'N/A'
                            
                            # Safe extraction of citation count
                            inline_links = article.get('inline_links', {})
                            cited_by_info = inline_links.get('cited_by', {})
                            cited_by = cited_by_info.get('total', 0) if isinstance(cited_by_info, dict) else 0
                            
                            # Handle missing link
                            link = self.safe_get_field(article, 'link', 'No URL available')
                            
                            # Additional metadata
                            publication = self.safe_get_field(pub_info, 'summary', 'N/A')
                            year = self.safe_get_field(article, 'year', 'N/A')
                            
                            self.results.append({
                                'title': title,
                                'authors': authors,
                                'publication': publication,
                                'year': year,
                                'cited_by': cited_by,
                                'link': link
                            })
                        
                        print(f"Success ({len(results['organic_results'])} articles)")
                        page_success = True
                    else:
                        print("No organic results on this page")
                        consecutive_errors += 1
                    
                    # Rate limiting between successful requests
                    if page_success and page < num_pages - 1:
                        time.sleep(delay)
                    
                except KeyError as e:
                    print(f"\nData structure error on page {page + 1}: Missing key '{e}'")
                    retry_count += 1
                    if retry_count < self.max_retries:
                        time.sleep(self.retry_delay)
                
                except ConnectionError:
                    print(f"\nConnection error on page {page + 1}")
                    retry_count += 1
                    if retry_count < self.max_retries:
                        wait_time = self.retry_delay * retry_count
                        print(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                
                except Exception as e:
                    print(f"\nUnexpected error on page {page + 1}: {str(e)}")
                    retry_count += 1
                    if retry_count < self.max_retries:
                        time.sleep(self.retry_delay)
            
            # If page failed after all retries
            if not page_success:
                print(f"Failed to fetch page {page + 1} after {self.max_retries} attempts")
                consecutive_errors += 1
        
        print(f"\n{'='*50}")
        print(f"Scraping completed!")
        print(f"Total articles collected: {len(self.results)}")
        print(f"{'='*50}\n")
        
        return pd.DataFrame(self.results) if self.results else pd.DataFrame()
    
    def save_to_csv(self, df, filename=None):
        """Save results to CSV with timestamp"""
        if df.empty:
            print("No data to save!")
            return None
            
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"scholar_results_{timestamp}.csv"
        
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"Results saved to {filename}")
            return filename
        except IOError as e:
            print(f"Error saving file: {e}")
            return None
        except Exception as e:
            print(f"Unexpected error while saving: {e}")
            return None


def get_user_input():
    """Get search parameters from user"""
    print("=" * 50)
    print("Google Scholar Article Scraper")
    print("=" * 50)
    
    # Get search query
    query = input("\nEnter your search query: ").strip()
    
    while not query:
        print("Search query cannot be empty!")
        query = input("Enter your search query: ").strip()
    
    # Get number of pages
    while True:
        try:
            num_pages = input("\nEnter number of pages to scrape (default: 3, max: 10): ").strip()
            
            if not num_pages:
                num_pages = 3
                break
            
            num_pages = int(num_pages)
            
            if num_pages < 1:
                print("Number of pages must be at least 1!")
                continue
            
            if num_pages > 10:
                print("Warning: Scraping many pages may hit rate limits.")
                confirm = input("Continue? (y/n): ").strip().lower()
                if confirm != 'y':
                    continue
            
            break
        except ValueError:
            print("Please enter a valid number!")
    
    return query, num_pages


if __name__ == "__main__":
    API_KEY = os.getenv('SERPAPI_KEY')
    
    if not API_KEY:
        print("=" * 50)
        print("ERROR: SERPAPI_KEY not found!")
        print("=" * 50)
        print("\nPlease ensure:")
        print("1. You have a .env file in the same directory")
        print("2. The file contains: SERPAPI_KEY=your_api_key_here")
        print("3. You have a valid SerpAPI key from https://serpapi.com/")
        print("=" * 50)
        sys.exit(1)
    
    try:
        # Get user input
        search_query, pages = get_user_input()
        
        # Create scraper and search
        scraper = GoogleScholarScraperSerpAPI(API_KEY)
        df = scraper.search_articles(search_query, num_pages=pages)
        
        if not df.empty:
            # Save results
            scraper.save_to_csv(df)
            
            # Display preview
            print(f"\n{'=' * 50}")
            print(f"Preview of results (first 5):")
            print(f"{'=' * 50}\n")
            
            # Display with better formatting
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', None)
            pd.set_option('display.max_colwidth', 50)
            
            print(df.head().to_string(index=False))
            
            # Display statistics
            print(f"\n{'=' * 50}")
            print("Statistics:")
            print(f"{'=' * 50}")
            print(f"Total articles: {len(df)}")
            print(f"Average citations: {df['cited_by'].mean():.2f}")
            print(f"Most cited: {df['cited_by'].max()}")
            print(f"Articles with missing authors: {(df['authors'] == 'N/A').sum()}")
            print(f"{'=' * 50}\n")
        else:
            print("\nNo results found or all requests failed!")
            print("Please check:")
            print("- Your search query")
            print("- API key validity")
            print("- Account credits on https://serpapi.com/dashboard")
    
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user!")
        sys.exit(0)
    
    except Exception as e:
        print(f"\n{'=' * 50}")
        print(f"FATAL ERROR")
        print(f"{'=' * 50}")
        print(f"Error: {str(e)}")
        print("\nPlease report this error with the full traceback.")
        print(f"{'=' * 50}\n")
        sys.exit(1)
