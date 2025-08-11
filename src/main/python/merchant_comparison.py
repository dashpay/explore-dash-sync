import pandas as pd
import numpy as np
import math
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from difflib import SequenceMatcher
import os
import sys
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import threading
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import re
import time
from datetime import datetime

# Initialize geocoder globally with rate limiting
geocoder = Nominatim(user_agent="MerchantComparison_v3", timeout=5)  # Reduced timeout

@lru_cache(maxsize=5000)  # Increased cache size
def reverse_geocode_cached(lat, lon):
    """Cached reverse geocoding to get city and state from coordinates"""
    try:
        # Round coordinates to reduce cache misses while maintaining accuracy
        rounded_lat = round(float(lat), 3)  # Reduced precision for better caching
        rounded_lon = round(float(lon), 3)
        
        location = geocoder.reverse((rounded_lat, rounded_lon), exactly_one=True)
        
        if location and location.address:
            address_parts = location.raw.get('address', {})
            
            # Extract city (try multiple possible keys)
            city = (address_parts.get('city') or 
                   address_parts.get('town') or 
                   address_parts.get('village') or 
                   address_parts.get('hamlet') or
                   address_parts.get('municipality') or
                   address_parts.get('county', '')).strip()
            
            # Extract state
            state = (address_parts.get('state') or 
                    address_parts.get('province', '')).strip()
            
            return city, state
            
    except Exception as e:
        # Return empty strings on any error
        pass
    
    return '', ''

def batch_reverse_geocode(coordinates, progress_callback=None):
    """Process multiple coordinates in batch with progress tracking"""
    results = []
    total = len(coordinates)
    
    for i, (lat, lon) in enumerate(coordinates):
        if pd.notna(lat) and pd.notna(lon):
            city, state = reverse_geocode_cached(lat, lon)
            results.append((city, state))
        else:
            results.append(('', ''))
        
        # Update progress
        if progress_callback and (i + 1) % 10 == 0:  # Update every 10 records
            progress_callback(i + 1, total)
        
        # Reduced sleep time for faster processing
        time.sleep(0.05)  # 50ms instead of 100ms
    
    return results

def clean_name_advanced_cached(name):
    """Cached version of advanced name cleaning for better performance"""
    if pd.isna(name) or not name:
        return ""
    
    # Convert to lowercase and strip
    cleaned = str(name).lower().strip()
    
    # Remove common business suffixes and prefixes
    suffixes = ['inc', 'llc', 'corp', 'ltd', 'co', 'company', 'corporation', 'limited']
    prefixes = ['the ', 'a ']
    
    # Remove punctuation and extra spaces
    cleaned = re.sub(r'[^\w\s]', ' ', cleaned)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # Remove suffixes
    words = cleaned.split()
    words = [w for w in words if w not in suffixes]
    
    # Remove prefixes
    cleaned = ' '.join(words)
    for prefix in prefixes:
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix):]
    
    return cleaned.strip()

@lru_cache(maxsize=50000)
def advanced_name_similarity_cached(name1, name2):
    """Cached advanced name similarity with optimizations"""
    if not name1 or not name2:
        return 0.0
    
    clean1 = clean_name_advanced_cached(name1)
    clean2 = clean_name_advanced_cached(name2)
    
    if not clean1 or not clean2:
        return 0.0
    
    # Quick exact match check
    if clean1 == clean2:
        return 1.0
    
    # Quick length difference check
    len_ratio = min(len(clean1), len(clean2)) / max(len(clean1), len(clean2))
    if len_ratio < 0.3:
        return 0.0
    
    # Sequence matcher
    seq_sim = SequenceMatcher(None, clean1, clean2).ratio()
    
    # Early exit if sequence similarity is very low
    if seq_sim < 0.3:
        return seq_sim * 0.4
    
    # Word overlap similarity
    words1 = set(clean1.split())
    words2 = set(clean2.split())
    if words1 and words2:
        word_overlap = len(words1.intersection(words2)) / len(words1.union(words2))
    else:
        word_overlap = 0.0
    
    # Check for exact word matches
    exact_word_match = any(word in clean2 for word in clean1.split() if len(word) > 3)
    exact_bonus = 0.3 if exact_word_match else 0.0
    
    # Weighted combination
    final_score = (seq_sim * 0.5 + word_overlap * 0.4 + exact_bonus)
    return min(final_score, 1.0)

def haversine_vectorized(lat1, lon1, lat2_array, lon2_array):
    """Vectorized haversine distance calculation"""
    # Convert to radians
    lat1_rad = np.radians(lat1)
    lon1_rad = np.radians(lon1)
    lat2_rad = np.radians(lat2_array)
    lon2_rad = np.radians(lon2_array)
    
    # Haversine formula
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = np.sin(dlat/2)**2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    
    # Earth's radius in miles
    return 3959 * c

def spatial_index_filter(piggy_lat, piggy_lon, ctx_df, max_distance_miles):
    """Fast spatial filtering using bounding box"""
    # Approximate degrees per mile
    lat_deg_per_mile = 1 / 69.0
    lon_deg_per_mile = 1 / (69.0 * np.cos(np.radians(piggy_lat)))
    
    # Create bounding box
    lat_min = piggy_lat - (max_distance_miles * lat_deg_per_mile)
    lat_max = piggy_lat + (max_distance_miles * lat_deg_per_mile)
    lon_min = piggy_lon - (max_distance_miles * lon_deg_per_mile)
    lon_max = piggy_lon + (max_distance_miles * lon_deg_per_mile)
    
    # Filter using bounding box
    mask = ((ctx_df['latitude'] >= lat_min) & (ctx_df['latitude'] <= lat_max) & 
            (ctx_df['longitude'] >= lon_min) & (ctx_df['longitude'] <= lon_max))
    
    return ctx_df[mask].copy()

@lru_cache(maxsize=3000)
def cities_match_enhanced(city1, city2):
    """Enhanced city matching with common abbreviations"""
    if pd.isna(city1) or pd.isna(city2):
        return False
    
    c1 = str(city1).lower().strip()
    c2 = str(city2).lower().strip()
    
    # Exact match
    if c1 == c2:
        return True
    
    # Common city abbreviations
    city_variations = {
        'saint': 'st',
        'mount': 'mt',
        'fort': 'ft',
        'north': 'n',
        'south': 's',
        'east': 'e',
        'west': 'w',
    }
    
    # Normalize city names
    for full, abbrev in city_variations.items():
        c1 = c1.replace(full, abbrev)
        c2 = c2.replace(full, abbrev)
    
    return c1 == c2

@lru_cache(maxsize=500)
def states_match_enhanced(state1, state2):
    """Enhanced state matching with abbreviations"""
    if pd.isna(state1) or pd.isna(state2):
        return False
    
    s1 = str(state1).upper().strip()
    s2 = str(state2).upper().strip()
    
    # Exact match
    if s1 == s2:
        return True
    
    # State abbreviation mapping
    state_abbrevs = {
        'ALABAMA': 'AL', 'ALASKA': 'AK', 'ARIZONA': 'AZ', 'ARKANSAS': 'AR',
        'CALIFORNIA': 'CA', 'COLORADO': 'CO', 'CONNECTICUT': 'CT', 'DELAWARE': 'DE',
        'FLORIDA': 'FL', 'GEORGIA': 'GA', 'HAWAII': 'HI', 'IDAHO': 'ID',
        'ILLINOIS': 'IL', 'INDIANA': 'IN', 'IOWA': 'IA', 'KANSAS': 'KS',
        'KENTUCKY': 'KY', 'LOUISIANA': 'LA', 'MAINE': 'ME', 'MARYLAND': 'MD',
        'MASSACHUSETTS': 'MA', 'MICHIGAN': 'MI', 'MINNESOTA': 'MN', 'MISSISSIPPI': 'MS',
        'MISSOURI': 'MO', 'MONTANA': 'MT', 'NEBRASKA': 'NE', 'NEVADA': 'NV',
        'NEW HAMPSHIRE': 'NH', 'NEW JERSEY': 'NJ', 'NEW MEXICO': 'NM', 'NEW YORK': 'NY',
        'NORTH CAROLINA': 'NC', 'NORTH DAKOTA': 'ND', 'OHIO': 'OH', 'OKLAHOMA': 'OK',
        'OREGON': 'OR', 'PENNSYLVANIA': 'PA', 'RHODE ISLAND': 'RI', 'SOUTH CAROLINA': 'SC',
        'SOUTH DAKOTA': 'SD', 'TENNESSEE': 'TN', 'TEXAS': 'TX', 'UTAH': 'UT',
        'VERMONT': 'VT', 'VIRGINIA': 'VA', 'WASHINGTON': 'WA', 'WEST VIRGINIA': 'WV',
        'WISCONSIN': 'WI', 'WYOMING': 'WY'
    }
    
    # Convert full names to abbreviations
    s1_abbrev = state_abbrevs.get(s1, s1)
    s2_abbrev = state_abbrevs.get(s2, s2)
    
    return s1_abbrev == s2_abbrev

@lru_cache(maxsize=2000)
def cities_match(city1, city2):
    """Cached city matching"""
    return cities_match_enhanced(city1, city2)

@lru_cache(maxsize=500)
def states_match(state1, state2):
    """Cached state matching"""
    return states_match_enhanced(state1, state2)

@lru_cache(maxsize=1000)
def zip_codes_match(zip1, zip2):
    """Enhanced ZIP code matching with 5-digit normalization"""
    if pd.isna(zip1) or pd.isna(zip2):
        return False
    
    # Convert to string and extract first 5 digits
    z1 = str(zip1).strip()[:5]
    z2 = str(zip2).strip()[:5]
    
    # Must be numeric and same
    try:
        return z1.isdigit() and z2.isdigit() and z1 == z2
    except:
        return False

@lru_cache(maxsize=5000)
def street_address_similarity(addr1, addr2):
    """Extract and compare only street address components"""
    if pd.isna(addr1) or pd.isna(addr2) or not addr1 or not addr2:
        return 0.0
    
    def extract_street_address(address):
        """Extract street address by removing city, state, zip patterns"""
        addr = str(address).strip()
        
        # Remove zip codes from the end using a simple approach
        if ',' in addr:
            parts = addr.split(',')
            # Check if last part looks like zip code
            last_part = parts[-1].strip()
            if last_part.replace('-', '').isdigit() and (len(last_part) == 5 or len(last_part) == 10):
                parts = parts[:-1]
            # Check if second to last looks like state
            if len(parts) > 1:
                second_last = parts[-1].strip()
                if len(second_last) == 2 and second_last.isalpha():
                    parts = parts[:-1]
            addr = ','.join(parts)
        
        # Take first part before any comma
        street_addr = addr.split(',')[0].strip()
        return street_addr
    
    street1 = extract_street_address(addr1)
    street2 = extract_street_address(addr2)
    
    if not street1 or not street2:
        return 0.0
    
    # Normalize for comparison
    street1 = re.sub(r'[^\w\s]', ' ', street1.lower())
    street2 = re.sub(r'[^\w\s]', ' ', street2.lower())
    street1 = re.sub(r'\s+', ' ', street1).strip()
    street2 = re.sub(r'\s+', ' ', street2).strip()
    
    return SequenceMatcher(None, street1, street2).ratio()

def truncate_coordinates(lat, lon, precision):
    """Truncate coordinates to specified decimal places instead of rounding"""
    if pd.isna(lat) or pd.isna(lon):
        return None, None
    
    # Convert to float and truncate by multiplying, flooring, then dividing
    multiplier = 10 ** precision
    truncated_lat = math.floor(float(lat) * multiplier) / multiplier
    truncated_lon = math.floor(float(lon) * multiplier) / multiplier
    
    return truncated_lat, truncated_lon

def coordinate_priority_matching(piggy_df, ctx_df, coordinate_precision, max_distance_miles=0.5, ignore_name = False, min_name_sim = 0.90):
    """Primary coordinate-based matching with exact precision using truncation"""
    coordinate_matches = []
    
    # Create coordinate lookup for CTX data using truncation
    ctx_coord_lookup = {}
    for idx, row in ctx_df.iterrows():
        truncated_lat, truncated_lon = truncate_coordinates(row['latitude'], row['longitude'], coordinate_precision)
        if truncated_lat is not None and truncated_lon is not None:
            coord_key = (truncated_lat, truncated_lon)
            if coord_key not in ctx_coord_lookup:
                ctx_coord_lookup[coord_key] = []
            ctx_coord_lookup[coord_key].append(idx)
    
    # Find exact coordinate matches using truncation
    for piggy_idx, piggy_row in piggy_df.iterrows():
        truncated_lat, truncated_lon = truncate_coordinates(piggy_row['latitude'], piggy_row['longitude'], coordinate_precision)
        if truncated_lat is not None and truncated_lon is not None:
            coord_key = (truncated_lat, truncated_lon)
            if coord_key in ctx_coord_lookup:
                for ctx_idx in ctx_coord_lookup[coord_key]:
                    ctx_row = ctx_df.iloc[ctx_idx]
                    
                    # Calculate exact distance
                    distance = haversine_vectorized(
                        piggy_row['latitude'], piggy_row['longitude'],
                        np.array([ctx_row['latitude']]), np.array([ctx_row['longitude']])
                    )[0]

                    name_sim = advanced_name_similarity_cached(piggy_row['name'], ctx_row['name'])
                    if ignore_name:
                        meets_name_sim = True
                    else:
                        meets_name_sim = name_sim >= min_name_sim
                    
                    # Only include if within max distance
                    if distance <= max_distance_miles and meets_name_sim:
                        coordinate_matches.append({
                            'piggy_index': piggy_idx,
                            'ctx_index': ctx_idx,
                            'distance_miles': distance,
                            'match_type': 'COORDINATE_EXACT',
                            'coordinate_precision': coordinate_precision
                        })
    
    return coordinate_matches

def calculate_confidence_score_new(distance, name_sim, street_addr_sim, piggy_row, ctx_row, 
                                 ignore_name=False, ignore_city=False, ignore_state=False, ignore_zip=False):
    """New confidence scoring that strictly prioritizes coordinates"""
    
    # Ultra-precise distance scoring for coordinate matching
    if distance <= 0.01:  # ~50 feet
        coordinate_score = 1.0
    elif distance <= 0.03:  # ~150 feet
        coordinate_score = 0.95
    elif distance <= 0.05:  # ~250 feet
        coordinate_score = 0.85
    elif distance <= 0.1:   # ~500 feet
        coordinate_score = 0.7
    elif distance <= 0.2:   # ~1000 feet
        coordinate_score = 0.5
    elif distance <= 0.5:   # ~2500 feet
        coordinate_score = 0.3
    else:
        coordinate_score = 0.1
    
    # Name matching (if not ignored)
    name_score = name_sim if not ignore_name else 1.0
    
    # Street address only when ignoring geographic components
    street_score = 0.0
    using_street_address = ignore_city or ignore_state or ignore_zip
    if using_street_address and street_addr_sim > 0:
        street_score = street_addr_sim
    
    # Geographic validation (only if not ignored)
    city_match = cities_match(piggy_row['city'], ctx_row['city']) if not ignore_city else True
    state_match = states_match(piggy_row['state'], ctx_row['territory']) if not ignore_state else True
    zip_match = zip_codes_match(piggy_row['zip'], ctx_row.get('zip', '')) if not ignore_zip else True
    
    # Calculate confidence with coordinate priority
    if ignore_name:
        # Coordinates + address only mode
        if using_street_address:
            confidence = (coordinate_score * 0.7 + street_score * 0.3)
        else:
            confidence = coordinate_score  # Pure coordinate matching
    else:
        # Standard mode with coordinate priority
        if using_street_address:
            confidence = (coordinate_score * 0.5 + name_score * 0.3 + street_score * 0.2)
        else:
            confidence = (coordinate_score * 0.6 + name_score * 0.4)
    
    # Geographic bonuses
    geo_bonus = 0.0
    if not ignore_city and city_match:
        geo_bonus += 0.05
    if not ignore_state and state_match:
        geo_bonus += 0.05
    if not ignore_zip and zip_match:
        geo_bonus += 0.02
    
    final_confidence = min(confidence + geo_bonus, 1.0)
    
    # Strict coordinate-based validation
    if coordinate_score < 0.5:  # >0.2 miles apart
        final_confidence = min(final_confidence, 0.4)
    elif coordinate_score < 0.7:  # >0.1 miles apart
        final_confidence = min(final_confidence, 0.6)
    
    return final_confidence

def create_comparison_report_advanced(piggy_df, ctx_df, all_matches, enable_reverse_geocoding=False):
    """Create detailed comparison report with corrected match type assignment"""
    results = []
    
    # Track used indices
    matched_piggy_indices = set()
    matched_ctx_indices = set()
    
    def get_corrected_location(lat, lon, enable_geocoding):
        """Get corrected city and state if reverse geocoding is enabled"""
        if enable_geocoding and not pd.isna(lat) and not pd.isna(lon):
            try:
                corrected_city, corrected_state = reverse_geocode_cached(lat, lon)
                return corrected_city, corrected_state
            except:
                return '', ''
        return '', ''
    
    # Add all matches
    for match in all_matches:
        piggy_row = piggy_df.iloc[match['piggy_index']]
        ctx_row = ctx_df.iloc[match['ctx_index']]
        
        # Get confidence and other match data
        confidence = match['confidence']
        city_match = match.get('city_match', False)
        state_match = match.get('state_match', False)
        zip_match = match.get('zip_match', True)
        geographic_warning = match.get('geographic_warning', '')
        
        # CORRECTED: Use the same confidence thresholds as the summary
        if confidence >= 0.9:
            match_type = 'HIGH_CONFIDENCE_DUPLICATE'
        elif confidence >= 0.7:
            match_type = 'MEDIUM_CONFIDENCE_DUPLICATE'
        elif confidence >= 0.5:
            match_type = 'LOW_CONFIDENCE_DUPLICATE'
        else:
            match_type = 'POTENTIAL_MATCH'
        
        # Add geographic context if relevant
        if geographic_warning:
            if 'different_states' in geographic_warning or 'state mismatch' in geographic_warning.lower():
                match_type += '_GEOGRAPHIC_WARNING'
        
        # Mark as matched for confidence >= 0.5 (instead of 0.8)
        if confidence >= 0.5:
            matched_piggy_indices.add(match['piggy_index'])
            matched_ctx_indices.add(match['ctx_index'])
        
        # Get corrected locations if enabled
        piggy_corrected_city, piggy_corrected_state = get_corrected_location(
            piggy_row['latitude'], piggy_row['longitude'], enable_reverse_geocoding)
        ctx_corrected_city, ctx_corrected_state = get_corrected_location(
            ctx_row['latitude'], ctx_row['longitude'], enable_reverse_geocoding)
        
        result_row = {
            'match_type': match_type,
            'confidence_score': confidence,
            'piggy_name': piggy_row['name'],
            'piggy_address': piggy_row['address1'],
            'piggy_city': piggy_row['city'],
            'piggy_state': piggy_row['territory'],
            'piggy_lat': piggy_row['latitude'],
            'piggy_lon': piggy_row['longitude'],
            'ctx_name': ctx_row['name'],
            'ctx_address': ctx_row['address1'],
            'ctx_city': ctx_row['city'],
            'ctx_territory': ctx_row['territory'],
            'ctx_lat': ctx_row['latitude'],
            'ctx_lon': ctx_row['longitude'],
            'distance_miles': match['distance_miles'],
            'name_similarity': match['name_similarity'],
            'address_similarity': match.get('address_similarity', 0),
            'city_match': city_match,
            'state_match': state_match,
            'zip_match': zip_match,
            'match_reasons': match.get('reasons', ''),
            'geographic_warning': geographic_warning
        }
        
        # Add corrected location columns if reverse geocoding is enabled
        if enable_reverse_geocoding:
            result_row.update({
                'piggy_corrected_city': piggy_corrected_city,
                'piggy_corrected_state': piggy_corrected_state,
                'ctx_corrected_city': ctx_corrected_city,
                'ctx_corrected_state': ctx_corrected_state
            })
        
        results.append(result_row)
    
    # Add unique Piggy locations
    for i, row in piggy_df.iterrows():
        if i not in matched_piggy_indices:
            # Get corrected location for unique Piggy entries
            piggy_corrected_city, piggy_corrected_state = get_corrected_location(
                row['latitude'], row['longitude'], enable_reverse_geocoding)

            result_row = {
                'match_type': 'PIGGY_UNIQUE',
                'confidence_score': 0,
                'piggy_name': row['name'],
                'piggy_address': row['address1'],
                'piggy_city': row['city'],
                'piggy_state': row['territory'],
                'piggy_lat': row['latitude'],
                'piggy_lon': row['longitude'],
                'ctx_name': '',
                'ctx_address': '',
                'ctx_city': '',
                'ctx_territory': '',
                'ctx_lat': '',
                'ctx_lon': '',
                'distance_miles': '',
                'name_similarity': '',
                'address_similarity': '',
                'city_match': False,
                'state_match': False,
                'zip_match': False,
                'match_reasons': '',
                'geographic_warning': ''
            }
            
            if enable_reverse_geocoding:
                result_row.update({
                    'piggy_corrected_city': piggy_corrected_city,
                    'piggy_corrected_state': piggy_corrected_state,
                    'ctx_corrected_city': '',
                    'ctx_corrected_state': ''
                })
            
            results.append(result_row)
    
    # Add unique CTX locations
    for i, row in ctx_df.iterrows():
        if i not in matched_ctx_indices:
            # Get corrected location for unique CTX entries
            ctx_corrected_city, ctx_corrected_state = get_corrected_location(
                row['latitude'], row['longitude'], enable_reverse_geocoding)
            
            result_row = {
                'match_type': 'CTX_UNIQUE',
                'confidence_score': 0,
                'piggy_name': '',
                'piggy_address': '',
                'piggy_city': '',
                'piggy_state': '',
                'piggy_lat': '',
                'piggy_lon': '',
                'ctx_name': row['name'],
                'ctx_address': row['address1'],
                'ctx_city': row['city'],
                'ctx_territory': row['territory'],
                'ctx_lat': row['latitude'],
                'ctx_lon': row['longitude'],
                'distance_miles': '',
                'name_similarity': '',
                'address_similarity': '',
                'city_match': False,
                'state_match': False,
                'zip_match': False,
                'match_reasons': '',
                'geographic_warning': ''
            }
            
            if enable_reverse_geocoding:
                result_row.update({
                    'piggy_corrected_city': '',
                    'piggy_corrected_state': '',
                    'ctx_corrected_city': ctx_corrected_city,
                    'ctx_corrected_state': ctx_corrected_state
                })
            
            results.append(result_row)
    
    return pd.DataFrame(results)

class MerchantComparisonGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Advanced Merchant Location Comparison Tool v3.0.1")
        self.root.geometry("950x900")
        self.root.resizable(True, True)
        
        # Dark mode theme variables
        self.dark_mode = tk.BooleanVar(value=False)
        self.setup_theme()
        
        # Settings file path
        self.settings_file = Path.home() / "merchant_comparison_settings.json"
        
        # Variables
        self.piggy_file = tk.StringVar()
        self.ctx_file = tk.StringVar()
        self.geocoding_file = tk.StringVar()
        self.output_dir = tk.StringVar(value=os.path.expanduser("~/Desktop"))
        self.max_distance = tk.DoubleVar(value=2.0)
        self.min_name_similarity = tk.DoubleVar(value=0.6)
        self.min_confidence = tk.DoubleVar(value=0.5)
        self.coordinate_precision = tk.IntVar(value=4)
        self.prioritize_coordinates = tk.BooleanVar(value=True)
        self.ignore_state_matching = tk.BooleanVar(value=False)
        self.ignore_city_matching = tk.BooleanVar(value=False)
        self.ignore_zip_matching = tk.BooleanVar(value=False)
        self.ignore_name_matching = tk.BooleanVar(value=False)
        self.include_address_matching = tk.BooleanVar(value=True)
        self.show_all_potential_matches = tk.BooleanVar(value=True)
        self.use_parallel_processing = tk.BooleanVar(value=True)
        self.batch_size = tk.IntVar(value=200)
        self.auto_open_results = tk.BooleanVar(value=True)
        self.remember_window_size = tk.BooleanVar(value=True)
        self.enable_reverse_geocoding = tk.BooleanVar(value=False)
        self.geocoding_batch_size = tk.IntVar(value=100)
        
        # Create widgets first
        self.create_widgets()
        
        # Load saved settings after widgets are created
        self.load_settings()
        
        # Apply theme after loading settings and creating widgets
        self.apply_theme()
        self.update_widget_colors(self.root)
        if hasattr(self, 'theme_button'):
            self.theme_button.config(text="☀️ Light Mode" if self.dark_mode.get() else "🌙 Dark Mode")
        
        # Save settings when window is closed
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def setup_theme(self):
        """Setup light and dark theme colors"""
        self.themes = {
            'light': {
                'bg': '#ffffff',
                'fg': '#000000',
                'entry_bg': '#ffffff',
                'entry_fg': '#000000',
                'button_bg': '#f0f0f0',
                'button_fg': '#000000',
                'frame_bg': '#ffffff',
                'text_bg': '#ffffff',
                'text_fg': '#000000',
                'select_bg': '#0078d4',
                'select_fg': '#ffffff'
            },
            'dark': {
                'bg': '#2d2d2d',
                'fg': '#ffffff',
                'entry_bg': '#2d2d2d',
                'entry_fg': '#ffffff',
                'button_bg': '#404040',
                'button_fg': '#000000',
                'frame_bg': '#2d2d2d',
                'text_bg': '#2d2d2d',
                'text_fg': '#ffffff',
                'select_bg': '#0078d4',
                'select_fg': '#ffffff'
            }
        }
        self.apply_theme()
    
    def apply_theme(self):
        """Apply the current theme to all widgets"""
        theme = self.themes['dark' if self.dark_mode.get() else 'light']
        
        # Configure root window
        self.root.configure(bg=theme['bg'])
        
        # Store theme for widget creation
        self.current_theme = theme
    
    def toggle_theme(self):
        """Toggle between light and dark mode"""
        self.dark_mode.set(not self.dark_mode.get())
        self.apply_theme()
        self.save_settings()
        
        # Update button text
        if hasattr(self, 'theme_button'):
            self.theme_button.config(text="☀️ Light Mode" if self.dark_mode.get() else "🌙 Dark Mode")
        
        # Update all existing widgets
        self.update_widget_colors(self.root)
    
    def update_widget_colors(self, widget):
        """Recursively update colors of all widgets"""
        theme = self.current_theme
        
        try:
            widget_class = widget.winfo_class()
            
            if widget_class in ['Frame', 'Toplevel']:
                widget.configure(bg=theme['frame_bg'])
                if hasattr(widget, 'configure') and 'fg' in widget.configure():
                    widget.configure(fg=theme['fg'])
            if widget.winfo_class() in ("Labelframe", "TLabelframe"):  # tk vs ttk
                try:  # ttk path
                    import tkinter.ttk as ttk
                    s = ttk.Style()
                    s.theme_use('clam')  # Aqua ignores colors
                    s.configure('Light.TLabelframe', background=theme['bg'])
                    s.configure('Light.TLabelframe.Label',
                                background=theme['bg'], foreground=theme['fg'])
                    widget.configure(style='Light.TLabelframe')
                except Exception:
                    widget.configure(bg=theme['bg'], fg=theme['fg'])  # tk.LabelFrame
            elif widget_class == 'Label':
                widget.configure(bg=theme['bg'], fg=theme['fg'])
            elif widget_class == 'Entry':
                widget.configure(bg=theme['entry_bg'], fg=theme['entry_fg'], 
                               insertbackground=theme['fg'])
            elif widget_class == 'Button':
                widget.configure(bg=theme['button_bg'], fg=theme['button_fg'])
            elif widget_class == 'Text':
                widget.configure(bg=theme['text_bg'], fg=theme['text_fg'], 
                               insertbackground=theme['fg'])
            elif widget_class == 'Checkbutton':
                widget.configure(bg=theme['frame_bg'], fg=theme['fg'], 
                               selectcolor=theme['entry_bg'])
            elif widget_class == 'Scale':
                widget.configure(bg=theme['bg'], fg=theme['fg'])
            elif widget_class == 'Canvas':
                widget.configure(bg=theme['bg'])
        except:
            pass  # Some widgets might not support certain options
        
        # Update special widgets if they exist
        if hasattr(self, 'canvas'):
            try:
                self.canvas.configure(bg=theme['bg'])
            except:
                pass
        
        if hasattr(self, 'scrollable_frame'):
            try:
                self.scrollable_frame.configure(bg=theme['frame_bg'])
            except:
                pass
        
        # Recursively update children
        for child in widget.winfo_children():
            self.update_widget_colors(child)
    
    def create_widgets(self):
        # Title
        title_label = tk.Label(self.root, text="Advanced Merchant Location Comparison Tool v3.0.1", 
                              font=("Arial", 16, "bold"))
        title_label.pack(pady=10)
        
        # Create main frame with scrollbar for the entire interface
        main_container = tk.Frame(self.root)
        main_container.pack(fill="both", expand=True, padx=5, pady=5)
        
        # Canvas for scrolling
        canvas = tk.Canvas(main_container, highlightthickness=0)
        v_scrollbar = ttk.Scrollbar(main_container, orient="vertical", command=canvas.yview)
        h_scrollbar = ttk.Scrollbar(main_container, orient="horizontal", command=canvas.xview)
        scrollable_frame = tk.Frame(canvas)
        
        # Store references for theming
        self.canvas = canvas
        self.scrollable_frame = scrollable_frame
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
        
        # Mouse wheel scrolling
        def _on_mousewheel(event):
            canvas.yview_scroll(int(-1*(event.delta/120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        
        # File selection frame
        file_frame = tk.LabelFrame(scrollable_frame, text="File Selection", font=("Arial", 10, "bold"))
        file_frame.pack(pady=5, padx=10, fill="x")
        
        # Piggy file selection
        tk.Label(file_frame, text="Piggy CSV File:", font=("Arial", 10, "bold")).pack(anchor="w", padx=10, pady=(10,0))
        piggy_frame = tk.Frame(file_frame)
        piggy_frame.pack(fill="x", pady=5, padx=10)
        piggy_entry = tk.Entry(piggy_frame, textvariable=self.piggy_file, width=60)
        piggy_entry.pack(side="left", fill="x", expand=True)
        tk.Button(piggy_frame, text="Browse", command=self.browse_piggy_file).pack(side="right", padx=(5,0))
        
        # CTX file selection
        tk.Label(file_frame, text="CTX CSV File:", font=("Arial", 10, "bold")).pack(anchor="w", padx=10, pady=(10,0))
        ctx_frame = tk.Frame(file_frame)
        ctx_frame.pack(fill="x", pady=5, padx=10)
        ctx_entry = tk.Entry(ctx_frame, textvariable=self.ctx_file, width=60)
        ctx_entry.pack(side="left", fill="x", expand=True)
        tk.Button(ctx_frame, text="Browse", command=self.browse_ctx_file).pack(side="right", padx=(5,0))
        
        # Output directory selection
        tk.Label(file_frame, text="Output Directory:", font=("Arial", 10, "bold")).pack(anchor="w", padx=10, pady=(10,0))
        output_frame = tk.Frame(file_frame)
        output_frame.pack(fill="x", pady=(5,10), padx=10)
        output_entry = tk.Entry(output_frame, textvariable=self.output_dir, width=60)
        output_entry.pack(side="left", fill="x", expand=True)
        tk.Button(output_frame, text="Browse", command=self.browse_output_dir).pack(side="right", padx=(5,0))
        
        # Separator
        separator = ttk.Separator(scrollable_frame, orient='horizontal')
        separator.pack(fill='x', pady=10, padx=10)
        
        # Geocoding only frame
        geocoding_frame = tk.LabelFrame(scrollable_frame, text="Reverse Geocoding Only", font=("Arial", 10, "bold"))
        geocoding_frame.pack(pady=5, padx=10, fill="x")
        
        tk.Label(geocoding_frame, text="Add corrected city/state to a single CSV file based on coordinates:", 
                font=("Arial", 9)).pack(anchor="w", padx=10, pady=(5,0))
        
        # Geocoding file selection
        tk.Label(geocoding_frame, text="CSV File for Geocoding:", font=("Arial", 10, "bold")).pack(anchor="w", padx=10, pady=(10,0))
        geocoding_file_frame = tk.Frame(geocoding_frame)
        geocoding_file_frame.pack(fill="x", pady=5, padx=10)
        geocoding_entry = tk.Entry(geocoding_file_frame, textvariable=self.geocoding_file, width=60)
        geocoding_entry.pack(side="left", fill="x", expand=True)
        tk.Button(geocoding_file_frame, text="Browse", command=self.browse_geocoding_file).pack(side="right", padx=(5,0))
        
        # Geocoding button
        geocoding_button_frame = tk.Frame(geocoding_frame)
        geocoding_button_frame.pack(pady=10)
        
        # Geocoding batch size setting
        batch_frame = tk.Frame(geocoding_frame)
        batch_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(batch_frame, text="Batch size for progress updates:").pack(side="left")
        batch_value_label = tk.Label(batch_frame, textvariable=self.geocoding_batch_size, font=("Arial", 9))
        batch_value_label.pack(side="right")
        batch_scale = tk.Scale(batch_frame, from_=10, to=500, resolution=10, orient="horizontal", 
                variable=self.geocoding_batch_size, command=self.save_settings_delayed)
        batch_scale.pack(side="right", fill="x", expand=True, padx=(10,10))
        
        self.geocoding_button = tk.Button(geocoding_button_frame, text="Add Geographic Data to File", 
                                         command=self.start_geocoding_only, bg="#2196F3", fg="white",
                                         font=("Arial", 10, "bold"), padx=20, pady=10)
        self.geocoding_button.pack()
        
        # Progress bar for geocoding
        self.geocoding_progress_var = tk.StringVar(value="Ready for geocoding")
        tk.Label(geocoding_frame, textvariable=self.geocoding_progress_var, font=("Arial", 9)).pack(pady=(5,0))
        self.geocoding_progress_bar = ttk.Progressbar(geocoding_frame, mode='determinate')
        self.geocoding_progress_bar.pack(pady=5, padx=10, fill="x")
        
        tk.Label(geocoding_frame, text="Note: Uses same output directory as above. Output will have '_geocoded' suffix.", 
                font=("Arial", 8), fg="gray").pack(pady=(0,10))
        
        # Advanced Settings frame
        settings_frame = tk.LabelFrame(scrollable_frame, text="Advanced Matching Settings", font=("Arial", 10, "bold"))
        settings_frame.pack(pady=5, padx=10, fill="x")
        
        # Max distance with intelligent defaults
        dist_frame = tk.Frame(settings_frame)
        dist_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(dist_frame, text="Max Distance (miles):").pack(side="left")
        dist_value_label = tk.Label(dist_frame, textvariable=self.max_distance, font=("Arial", 10, "bold"))
        dist_value_label.pack(side="right")
        dist_scale = tk.Scale(dist_frame, from_=0.1, to=25.0, resolution=0.1, orient="horizontal", 
                variable=self.max_distance, command=self.save_settings_delayed)
        dist_scale.pack(side="right", fill="x", expand=True, padx=(10,10))
        
        tk.Label(settings_frame, text="💡 Coordinate Mode: Use 0.1-0.5 miles when ignoring city/state/zip", 
                font=("Arial", 8), fg="blue").pack(anchor="w", padx=10)
        tk.Label(settings_frame, text="💡 Geographic Mode: Use 2-10 miles for city/state matching", 
                font=("Arial", 8), fg="gray").pack(anchor="w", padx=10)
        
        # Name similarity threshold
        name_frame = tk.Frame(settings_frame)
        name_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(name_frame, text="Min Name Similarity:").pack(side="left")
        name_value_label = tk.Label(name_frame, textvariable=self.min_name_similarity, font=("Arial", 10, "bold"))
        name_value_label.pack(side="right")
        name_scale = tk.Scale(name_frame, from_=0.3, to=1.0, resolution=0.05, orient="horizontal", 
                variable=self.min_name_similarity, command=self.save_settings_delayed)
        name_scale.pack(side="right", fill="x", expand=True, padx=(10,10))
        
        # Confidence threshold
        conf_frame = tk.Frame(settings_frame)
        conf_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(conf_frame, text="Min Confidence Score:").pack(side="left")
        conf_value_label = tk.Label(conf_frame, textvariable=self.min_confidence, font=("Arial", 10, "bold"))
        conf_value_label.pack(side="right")
        conf_scale = tk.Scale(conf_frame, from_=0.3, to=1.0, resolution=0.05, orient="horizontal", 
                variable=self.min_confidence, command=self.save_settings_delayed)
        conf_scale.pack(side="right", fill="x", expand=True, padx=(10,10))
        
        # Coordinate precision setting
        coord_frame = tk.Frame(settings_frame)
        coord_frame.pack(fill="x", padx=10, pady=5)
        tk.Label(coord_frame, text="Coordinate Precision (decimal places):").pack(side="left")
        coord_value_label = tk.Label(coord_frame, textvariable=self.coordinate_precision, font=("Arial", 10, "bold"))
        coord_value_label.pack(side="right")
        coord_scale = tk.Scale(coord_frame, from_=2, to=6, resolution=1, orient="horizontal", 
                variable=self.coordinate_precision, command=self.save_settings_delayed)
        coord_scale.pack(side="right", fill="x", expand=True, padx=(10,10))
        
        # Options frame
        options_frame = tk.LabelFrame(scrollable_frame, text="Analysis Options", font=("Arial", 10, "bold"))
        options_frame.pack(pady=5, padx=10, fill="x")
        
        # Checkboxes for options
        options_inner = tk.Frame(options_frame)
        options_inner.pack(padx=10, pady=5)
        
        tk.Checkbutton(options_inner, text="Prioritize coordinate matching (recommended)", 
                      variable=self.prioritize_coordinates, command=self.save_settings).pack(anchor="w")
        
        # Geographic matching options frame
        geo_frame = tk.LabelFrame(options_inner, text="Geographic Matching Options", font=("Arial", 9, "bold"))
        geo_frame.pack(fill="x", pady=(10,5))
        
        geo_inner = tk.Frame(geo_frame)
        geo_inner.pack(padx=10, pady=5)
        
        tk.Checkbutton(geo_inner, text="Ignore state/territory for matching", 
                      variable=self.ignore_state_matching, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(geo_inner, text="Ignore city for matching", 
                      variable=self.ignore_city_matching, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(geo_inner, text="Ignore ZIP code for matching", 
                      variable=self.ignore_zip_matching, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(geo_inner, text="Ignore name for matching (coordinates + address only)", 
                      variable=self.ignore_name_matching, command=self.save_settings).pack(anchor="w")
        
        tk.Label(geo_inner, text="💡 Tip: Enable these for broad geographic matching or inconsistent data", 
                font=("Arial", 8), fg="gray").pack(anchor="w", pady=(5,0))
        
        tk.Checkbutton(options_inner, text="Include address similarity matching", 
                      variable=self.include_address_matching, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(options_inner, text="Show all potential matches (not just best)", 
                      variable=self.show_all_potential_matches, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(options_inner, text="Use parallel processing (faster for large datasets)", 
                      variable=self.use_parallel_processing, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(options_inner, text="Auto-open results file when complete", 
                      variable=self.auto_open_results, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(options_inner, text="Remember window size and position", 
                      variable=self.remember_window_size, command=self.save_settings).pack(anchor="w")
        tk.Checkbutton(options_inner, text="Enable reverse geocoding (corrected city/state from coordinates)", 
                      variable=self.enable_reverse_geocoding, command=self.save_settings).pack(anchor="w")
        
        tk.Label(options_inner, text="⚠️ Warning: Reverse geocoding adds significant processing time", 
                font=("Arial", 8), fg="red").pack(anchor="w", padx=20)
        
        # Performance settings
        perf_frame = tk.Frame(options_inner)
        perf_frame.pack(fill="x", pady=(5,0))
        tk.Label(perf_frame, text="Batch size (larger = faster):").pack(side="left")
        batch_scale = tk.Scale(perf_frame, from_=50, to=500, resolution=50, orient="horizontal", 
                variable=self.batch_size, command=self.save_settings_delayed)
        batch_scale.pack(side="right", fill="x", expand=True, padx=(10,0))
        tk.Label(perf_frame, textvariable=self.batch_size, font=("Arial", 9)).pack(side="right")
        
        # Progress frame
        progress_frame = tk.Frame(scrollable_frame)
        progress_frame.pack(pady=5, padx=10, fill="x")
        
        self.progress_var = tk.StringVar(value="Ready to start advanced comparison")
        tk.Label(progress_frame, textvariable=self.progress_var).pack(pady=2)
        self.progress_bar = ttk.Progressbar(progress_frame, mode='indeterminate')
        self.progress_bar.pack(pady=2, fill="x")
        
        # Buttons
        button_frame = tk.Frame(scrollable_frame)
        button_frame.pack(pady=15, fill="x")
        
        # Create a centered frame for buttons
        center_frame = tk.Frame(button_frame)
        center_frame.pack()
        
        self.compare_button = tk.Button(center_frame, text="Start Advanced Analysis", 
                                       command=self.start_comparison, bg="#4CAF50", fg="white",
                                       font=("Arial", 12, "bold"), padx=30, pady=15)
        self.compare_button.pack(side="left", padx=5)
        
        tk.Button(center_frame, text="Clear All Fields", command=self.clear_fields,
                 bg="#FF9800", fg="white", font=("Arial", 12, "bold"), 
                 padx=30, pady=15).pack(side="left", padx=5)
        
        tk.Button(center_frame, text="Exit", command=self.on_closing,
                 bg="#f44336", fg="white", font=("Arial", 12, "bold"), 
                 padx=30, pady=15).pack(side="left", padx=5)
        
        self.theme_button = tk.Button(center_frame, text="🌙 Dark Mode", 
                 command=self.toggle_theme, bg="#9C27B0", fg="white", 
                 font=("Arial", 12, "bold"), padx=30, pady=15)
        self.theme_button.pack(side="left", padx=5)
        
        # Results text area
        results_frame = tk.LabelFrame(scrollable_frame, text="Analysis Results", font=("Arial", 10, "bold"))
        results_frame.pack(pady=5, padx=10, fill="both", expand=True)
        
        # Create text widget with scrollbar
        text_frame = tk.Frame(results_frame)
        text_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        self.results_text = tk.Text(text_frame, height=15, wrap="word", font=("Consolas", 9))
        text_scrollbar = tk.Scrollbar(text_frame, orient="vertical", command=self.results_text.yview)
        self.results_text.configure(yscrollcommand=text_scrollbar.set)
        
        self.results_text.pack(side="left", fill="both", expand=True)
        text_scrollbar.pack(side="right", fill="y")
        
        # Status bar
        self.status_var = tk.StringVar(value="Advanced analysis ready")
        status_bar = tk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor="w")
        status_bar.pack(side="bottom", fill="x")
        
        # Pack canvas and scrollbars
        canvas.pack(side="left", fill="both", expand=True)
        v_scrollbar.pack(side="right", fill="y")
        h_scrollbar.pack(side="bottom", fill="x")
    
    def load_settings(self):
        """Load settings from file"""
        try:
            if self.settings_file.exists():
                with open(self.settings_file, 'r') as f:
                    settings = json.load(f)
                
                # Load file paths
                self.piggy_file.set(settings.get('piggy_file', ''))
                self.ctx_file.set(settings.get('ctx_file', ''))
                self.output_dir.set(settings.get('output_dir', os.path.expanduser("~/Desktop")))
                
                # Load comparison settings
                self.max_distance.set(settings.get('max_distance', 2.0))
                self.min_name_similarity.set(settings.get('min_name_similarity', 0.6))
                self.min_confidence.set(settings.get('min_confidence', 0.5))
                self.coordinate_precision.set(settings.get('coordinate_precision', 4))
                
                # Load options
                self.prioritize_coordinates.set(settings.get('prioritize_coordinates', True))
                self.ignore_state_matching.set(settings.get('ignore_state_matching', False))
                self.ignore_city_matching.set(settings.get('ignore_city_matching', False))
                self.ignore_zip_matching.set(settings.get('ignore_zip_matching', False))
                self.ignore_name_matching.set(settings.get('ignore_name_matching', False))
                self.include_address_matching.set(settings.get('include_address_matching', True))
                self.show_all_potential_matches.set(settings.get('show_all_potential_matches', True))
                self.auto_open_results.set(settings.get('auto_open_results', True))
                self.remember_window_size.set(settings.get('remember_window_size', True))
                self.use_parallel_processing.set(settings.get('use_parallel_processing', True))
                self.batch_size.set(settings.get('batch_size', 200))
                self.enable_reverse_geocoding.set(settings.get('enable_reverse_geocoding', False))
                self.geocoding_file.set(settings.get('geocoding_file', ''))
                self.geocoding_batch_size.set(settings.get('geocoding_batch_size', 100))
                self.dark_mode.set(settings.get('dark_mode', False))
                
                # Load window settings if enabled
                if settings.get('remember_window_size', True):
                    window_geometry = settings.get('window_geometry', '950x900')
                    self.root.geometry(window_geometry)
                
                self.log_message("Settings loaded from previous session")
                if hasattr(self, 'status_var'):
                    self.status_var.set("Settings loaded successfully")
            else:
                self.log_message("No previous settings found - using defaults")
                if hasattr(self, 'status_var'):
                    self.status_var.set("Using default settings")
        except Exception as e:
            self.log_message(f"Error loading settings: {str(e)}")
            if hasattr(self, 'status_var'):
                self.status_var.set(f"Error loading settings: {str(e)}")
    
    def save_settings(self, event=None):
        """Save current settings to file"""
        try:
            settings = {
                'piggy_file': self.piggy_file.get(),
                'ctx_file': self.ctx_file.get(),
                'geocoding_file': self.geocoding_file.get(),
                'output_dir': self.output_dir.get(),
                'max_distance': self.max_distance.get(),
                'min_name_similarity': self.min_name_similarity.get(),
                'min_confidence': self.min_confidence.get(),
                'coordinate_precision': self.coordinate_precision.get(),
                'prioritize_coordinates': self.prioritize_coordinates.get(),
                'ignore_state_matching': self.ignore_state_matching.get(),
                'ignore_city_matching': self.ignore_city_matching.get(),
                'ignore_zip_matching': self.ignore_zip_matching.get(),
                'ignore_name_matching': self.ignore_name_matching.get(),
                'include_address_matching': self.include_address_matching.get(),
                'show_all_potential_matches': self.show_all_potential_matches.get(),
                'auto_open_results': self.auto_open_results.get(),
                'remember_window_size': self.remember_window_size.get(),
                'use_parallel_processing': self.use_parallel_processing.get(),
                'batch_size': self.batch_size.get(),
                'enable_reverse_geocoding': self.enable_reverse_geocoding.get(),
                'geocoding_batch_size': self.geocoding_batch_size.get(),
                'dark_mode': self.dark_mode.get(),
                'window_geometry': self.root.geometry() if self.remember_window_size.get() else '950x900'
            }
            
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
            
            self.status_var.set("Settings saved")
        except Exception as e:
            self.status_var.set(f"Error saving settings: {str(e)}")
    
    def save_settings_delayed(self, event=None):
        """Save settings with a small delay to avoid excessive saves during slider movement"""
        if hasattr(self, '_save_timer'):
            self.root.after_cancel(self._save_timer)
        self._save_timer = self.root.after(500, self.save_settings)
    
    def clear_fields(self):
        """Clear all file fields"""
        self.piggy_file.set('')
        self.ctx_file.set('')
        self.geocoding_file.set('')
        self.results_text.delete(1.0, tk.END)
        self.save_settings()
        self.log_message("All fields cleared")
    
    def browse_piggy_file(self):
        filename = filedialog.askopenfilename(
            title="Select Piggy CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.piggy_file.get()) if self.piggy_file.get() else None
        )
        if filename:
            self.piggy_file.set(filename)
            self.save_settings()
    
    def browse_ctx_file(self):
        filename = filedialog.askopenfilename(
            title="Select CTX CSV File",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.ctx_file.get()) if self.ctx_file.get() else None
        )
        if filename:
            self.ctx_file.set(filename)
            self.save_settings()
    
    def browse_output_dir(self):
        directory = filedialog.askdirectory(
            title="Select Output Directory",
            initialdir=self.output_dir.get()
        )
        if directory:
            self.output_dir.set(directory)
            self.save_settings()
    
    def browse_geocoding_file(self):
        filename = filedialog.askopenfilename(
            title="Select CSV File for Geocoding",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialdir=os.path.dirname(self.geocoding_file.get()) if self.geocoding_file.get() else None
        )
        if filename:
            self.geocoding_file.set(filename)
            self.save_settings()
    
    def log_message(self, message):
        # Only log if the results_text widget exists
        if hasattr(self, 'results_text'):
            self.results_text.insert(tk.END, message + "\n")
            self.results_text.see(tk.END)
            self.root.update_idletasks()
        else:
            # If widget doesn't exist yet, print to console as fallback
            print(message)
    
    def start_comparison(self):
        # Validate inputs
        if not self.piggy_file.get() or not self.ctx_file.get():
            messagebox.showerror("Error", "Please select both CSV files")
            return
        
        if not os.path.exists(self.piggy_file.get()):
            messagebox.showerror("Error", f"Piggy CSV file not found:\n{self.piggy_file.get()}")
            return
            
        if not os.path.exists(self.ctx_file.get()):
            messagebox.showerror("Error", f"CTX CSV file not found:\n{self.ctx_file.get()}")
            return
        
        # Start comparison in separate thread to prevent GUI freezing
        self.compare_button.config(state="disabled")
        self.progress_bar.start()
        
        thread = threading.Thread(target=self.run_advanced_comparison)
        thread.daemon = True
        thread.start()
    
    def start_geocoding_only(self):
        # Validate input
        if not self.geocoding_file.get():
            messagebox.showerror("Error", "Please select a CSV file for geocoding")
            return
        
        if not os.path.exists(self.geocoding_file.get()):
            messagebox.showerror("Error", f"CSV file not found:\n{self.geocoding_file.get()}")
            return
        
        # Start geocoding in separate thread
        self.geocoding_button.config(state="disabled")
        self.geocoding_progress_bar['value'] = 0
        self.geocoding_progress_var.set("Starting geocoding...")
        
        thread = threading.Thread(target=self.run_geocoding_only)
        thread.daemon = True
        thread.start()
    
    def run_advanced_comparison(self):
        try:
            start_time = time.time()
            self.progress_var.set("Loading and analyzing CSV files...")
            
            # Load files
            piggy_df = pd.read_csv(self.piggy_file.get())
            ctx_df = pd.read_csv(self.ctx_file.get())
            
            self.log_message(f"Loaded {len(piggy_df)} records from Piggy file")
            self.log_message(f"Loaded {len(ctx_df)} records from CTX file")
            
            # Data quality analysis
            self.progress_var.set("Analyzing data quality...")

            piggy_valid_coords = piggy_df.dropna(subset=['latitude', 'longitude']).reset_index(drop=True)
            ctx_valid_coords = ctx_df.dropna(subset=['latitude', 'longitude']).reset_index(drop=True)

            self.log_message(f"Data quality check:")
            self.log_message(f"  Piggy: {len(piggy_valid_coords)}/{len(piggy_df)} have valid coordinates")
            self.log_message(f"  CTX: {len(ctx_valid_coords)}/{len(ctx_df)} have valid coordinates")
            
            # Advanced matching analysis
            self.progress_var.set("Performing coordinate-priority matching...")
            
            all_matches = self.find_matches_advanced(piggy_valid_coords, ctx_valid_coords)
            
            processing_time = time.time() - start_time
            self.log_message(f"Found {len(all_matches)} potential matches in {processing_time:.1f} seconds")
            
            # Create detailed comparison report
            self.progress_var.set("Creating detailed comparison report...")
            
            # Check if reverse geocoding is enabled
            enable_geocoding = self.enable_reverse_geocoding.get()
            if enable_geocoding:
                self.log_message("Reverse geocoding enabled - this may take additional time...")
                # Add rate limiting delay for geocoding API
                time.sleep(0.1)
            
            comparison_df = create_comparison_report_advanced(piggy_valid_coords, ctx_valid_coords, all_matches, enable_geocoding)
            
            # Save results with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename_suffix = "_with_geocoding" if enable_geocoding else ""
            output_file = os.path.join(self.output_dir.get(), f"coordinate_priority_comparison_{timestamp}{filename_suffix}.csv")
            comparison_df.to_csv(output_file, index=False)
            
            # Detailed analysis summary
            high_conf = len([m for m in all_matches if m['confidence'] >= 0.9])
            medium_conf = len([m for m in all_matches if 0.7 <= m['confidence'] < 0.9])
            low_conf = len([m for m in all_matches if 0.5 <= m['confidence'] < 0.7])
            potential = len([m for m in all_matches if m['confidence'] < 0.5])
            
            unique_piggy = len(comparison_df[comparison_df['match_type'] == 'PIGGY_UNIQUE'])
            unique_ctx = len(comparison_df[comparison_df['match_type'] == 'CTX_UNIQUE'])
            
            total_time = time.time() - start_time
            
            self.log_message("\n" + "="*70)
            self.log_message("COORDINATE-PRIORITY ANALYSIS COMPLETE!")
            self.log_message("="*70)
            self.log_message(f"Total processing time: {total_time:.1f} seconds")
            self.log_message(f"\nMatch Quality Breakdown:")
            self.log_message(f"  High confidence duplicates (≥90%): {high_conf}")
            self.log_message(f"  Medium confidence duplicates (70-89%): {medium_conf}")
            self.log_message(f"  Low confidence duplicates (50-69%): {low_conf}")
            self.log_message(f"  Potential matches (<50%): {potential}")
            self.log_message(f"\nUnique Locations:")
            self.log_message(f"  Unique to Piggy: {unique_piggy}")
            self.log_message(f"  Unique to CTX: {unique_ctx}")
            
            if enable_geocoding:
                self.log_message(f"\nReverse geocoding completed - corrected city/state columns added")
            
            self.log_message(f"\nResults saved to: {output_file}")
            
            # Show sample high-confidence matches
            if high_conf > 0:
                self.log_message("\nSample high-confidence duplicates:")
                high_conf_matches = [m for m in all_matches if m['confidence'] >= 0.9][:3]
                for match in high_conf_matches:
                    piggy_row = piggy_valid_coords.iloc[match['piggy_index']]
                    ctx_row = ctx_valid_coords.iloc[match['ctx_index']]
                    self.log_message(f"  • {piggy_row['name']} ↔ {ctx_row['name']}")
                    self.log_message(f"    Distance: {match['distance_miles']:.3f} mi, Confidence: {match['confidence']:.1%}")
            
            self.progress_var.set("Coordinate-priority analysis complete!")
            
            # Auto-open results if enabled
            if self.auto_open_results.get():
                try:
                    os.startfile(output_file)  # Windows
                except:
                    try:
                        os.system(f'open "{output_file}"')  # macOS
                    except:
                        pass  # Linux or other
            
            messagebox.showinfo("Advanced Analysis Complete", 
                f"Analysis complete in {total_time:.1f} seconds!\n\n"
                f"High confidence duplicates: {high_conf}\n"
                f"Medium confidence duplicates: {medium_conf}\n"
                f"Low confidence duplicates: {low_conf}\n"
                f"Potential matches: {potential}\n\n"
                f"Unique to Piggy: {unique_piggy}\n"
                f"Unique to CTX: {unique_ctx}\n\n"
                f"Results saved to:\n{os.path.basename(output_file)}")
            
        except Exception as e:
            self.log_message(f"Error: {str(e)}")
            messagebox.showerror("Error", f"An error occurred:\n{str(e)}")
        
        finally:
            self.progress_bar.stop()
            self.compare_button.config(state="normal")
            self.progress_var.set("Ready for next analysis")
    
    def run_geocoding_only(self):
        try:
            start_time = time.time()
            self.geocoding_progress_var.set("Loading CSV file for geocoding...")
            
            # Load file
            df = pd.read_csv(self.geocoding_file.get())
            self.log_message(f"Loaded {len(df)} records for geocoding")
            
            # Detect coordinate columns
            lat_col = None
            lon_col = None
            
            # Common latitude column names
            lat_candidates = ['latitude', 'latitude', 'latitude', 'Latitude', 'latitude', 'LATITUDE']
            lon_candidates = ['longitude', 'lng', 'longitude', 'longitude', 'Lng', 'Longitude', 'longitude', 'LNG', 'LONGITUDE']

            for col in df.columns:
                if col in lat_candidates:
                    lat_col = col
                elif col in lon_candidates:
                    lon_col = col
            
            if not lat_col or not lon_col:
                messagebox.showerror("Error", 
                    f"Could not find latitude/longitude columns.\n"
                    f"Looking for columns named: {', '.join(lat_candidates + lon_candidates)}\n"
                    f"Found columns: {', '.join(df.columns.tolist())}")
                return
            
            self.log_message(f"Using columns: {lat_col}, {lon_col}")
            
            # Filter valid coordinates
            valid_coords = df.dropna(subset=[lat_col, lon_col])
            self.log_message(f"Found {len(valid_coords)} records with valid coordinates")
            
            if len(valid_coords) == 0:
                messagebox.showerror("Error", "No valid coordinates found in the file")
                return
            
            # Set up progress bar
            total_records = len(df)
            self.geocoding_progress_bar['maximum'] = total_records
            self.geocoding_progress_var.set("Starting reverse geocoding...")
            
            # Prepare coordinate pairs for batch processing
            coordinates = [(row[lat_col] if pd.notna(row[lat_col]) else None, 
                           row[lon_col] if pd.notna(row[lon_col]) else None) 
                          for _, row in df.iterrows()]
            
            # Progress callback function
            def update_progress(current, total):
                self.geocoding_progress_bar['value'] = current
                percentage = (current / total) * 100
                self.geocoding_progress_var.set(f"Geocoding... {current}/{total} ({percentage:.1f}%)")
                
                # Also log progress at certain intervals
                if current % self.geocoding_batch_size.get() == 0:
                    self.log_message(f"Processed {current}/{total} records ({percentage:.1f}%)")
                
                # Update GUI
                self.root.update_idletasks()
            
            # Perform batch reverse geocoding
            self.log_message("Starting optimized reverse geocoding process...")
            
            # Process in smaller chunks for better performance
            chunk_size = 50  # Process 50 records at a time
            all_results = []
            
            for i in range(0, len(coordinates), chunk_size):
                chunk = coordinates[i:i + chunk_size]
                
                # Process this chunk
                chunk_results = []
                for j, (lat, lon) in enumerate(chunk):
                    if lat is not None and lon is not None:
                        city, state = reverse_geocode_cached(lat, lon)
                        chunk_results.append((city, state))
                    else:
                        chunk_results.append(('', ''))
                    
                    # Update progress
                    current_record = i + j + 1
                    if current_record % 10 == 0 or current_record == total_records:
                        update_progress(current_record, total_records)
                    
                    # Faster rate limiting - only 20ms delay
                    time.sleep(0.02)
                
                all_results.extend(chunk_results)
                
                # Small pause between chunks to prevent overwhelming the service
                if i + chunk_size < len(coordinates):
                    time.sleep(0.1)
            
            # Ensure final progress update
            update_progress(total_records, total_records)
            
            # Add corrected columns to dataframe
            corrected_cities = [result[0] for result in all_results]
            corrected_states = [result[1] for result in all_results]
            
            df['corrected_city'] = corrected_cities
            df['corrected_state'] = corrected_states
            
            # Save results
            self.geocoding_progress_var.set("Saving geocoded results...")
            
            # Create output filename
            input_filename = os.path.basename(self.geocoding_file.get())
            name_without_ext = os.path.splitext(input_filename)[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_filename = f"{name_without_ext}_geocoded_{timestamp}.csv"
            output_file = os.path.join(self.output_dir.get(), output_filename)
            
            df.to_csv(output_file, index=False)
            
            processing_time = time.time() - start_time
            valid_geocoded = len([city for city in corrected_cities if city.strip()])
            processing_rate = len(df) / processing_time if processing_time > 0 else 0
            
            self.log_message("\n" + "="*50)
            self.log_message("REVERSE GEOCODING COMPLETE!")
            self.log_message("="*50)
            self.log_message(f"Total processing time: {processing_time:.1f} seconds")
            self.log_message(f"Processing rate: {processing_rate:.1f} records/second")
            self.log_message(f"Total records processed: {len(df)}")
            self.log_message(f"Successfully geocoded: {valid_geocoded}")
            self.log_message(f"Failed to geocode: {len(df) - valid_geocoded}")
            self.log_message(f"Cache hit rate: {(geocoder.cache_info().hits / (geocoder.cache_info().hits + geocoder.cache_info().misses) * 100):.1f}%" if hasattr(geocoder, 'cache_info') else "Cache info not available")
            self.log_message(f"Results saved to: {output_file}")
            
            self.geocoding_progress_var.set("Reverse geocoding complete!")
            
            # Auto-open results if enabled
            if self.auto_open_results.get():
                try:
                    os.startfile(output_file)  # Windows
                except:
                    try:
                        os.system(f'open "{output_file}"')  # macOS
                    except:
                        pass  # Linux or other
            
            messagebox.showinfo("Geocoding Complete", 
                f"Reverse geocoding complete in {processing_time:.1f} seconds!\n\n"
                f"Processing rate: {processing_rate:.1f} records/second\n"
                f"Total records: {len(df)}\n"
                f"Successfully geocoded: {valid_geocoded}\n"
                f"Failed to geocode: {len(df) - valid_geocoded}\n\n"
                f"Results saved to:\n{output_filename}")
            
        except Exception as e:
            self.log_message(f"Error during geocoding: {str(e)}")
            messagebox.showerror("Error", f"An error occurred during geocoding:\n{str(e)}")
        
        finally:
            self.geocoding_progress_bar['value'] = 0
            self.geocoding_button.config(state="normal")
            self.geocoding_progress_var.set("Ready for geocoding")
    
    def find_matches_advanced(self, piggy_df, ctx_df):
        """New coordinate-priority matching algorithm"""
        max_distance = self.max_distance.get()
        min_name_sim = self.min_name_similarity.get()
        min_confidence = self.min_confidence.get()
        include_address = self.include_address_matching.get()
        show_all_matches = self.show_all_potential_matches.get()
        coordinate_precision = self.coordinate_precision.get()
        
        ignore_state = self.ignore_state_matching.get()
        ignore_city = self.ignore_city_matching.get()
        ignore_zip = self.ignore_zip_matching.get()
        ignore_name = self.ignore_name_matching.get()
        
        self.log_message("Starting coordinate-priority matching algorithm...")
        
        # STEP 1: PRIMARY COORDINATE MATCHING (always first)
        self.log_message(f"Step 1: Finding truncated coordinate matches (precision: {coordinate_precision} decimal places)")
        exact_coordinate_matches = coordinate_priority_matching(piggy_df, ctx_df, coordinate_precision, max_distance, ignore_name, min_name_sim)

        all_matches = []
        matched_piggy_indices = set()
        matched_ctx_indices = set()
        
        # Process exact coordinate matches
        for coord_match in exact_coordinate_matches:
            piggy_idx = coord_match['piggy_index']
            ctx_idx = coord_match['ctx_index']
            distance = coord_match['distance_miles']
            
            piggy_row = piggy_df.iloc[piggy_idx]
            ctx_row = ctx_df.iloc[ctx_idx]
            
            # Calculate name similarity (if not ignored)
            name_sim = 0.0 if ignore_name else advanced_name_similarity_cached(piggy_row['name'], ctx_row['name'])
            
            # Calculate street address similarity (only when ignoring geographic components)
            street_addr_sim = 0.0
            if include_address and (ignore_city or ignore_state or ignore_zip):
                street_addr_sim = street_address_similarity(piggy_row['address1'], ctx_row['address1'])

            # Calculate confidence using new algorithm
            confidence = calculate_confidence_score_new(
                distance, name_sim, street_addr_sim, piggy_row, ctx_row,
                ignore_name, ignore_city, ignore_state, ignore_zip
            )
            
            # Apply minimum thresholds
            if not ignore_name and name_sim < min_name_sim:
                continue
            
            if confidence < min_confidence:
                continue
            
            # Create match record
            match_info = {
                'piggy_index': piggy_idx,
                'ctx_index': ctx_idx,
                'distance_miles': distance,
                'name_similarity': name_sim,
                'address_similarity': street_addr_sim,
                'confidence': confidence,
                'reasons': f"truncated_coordinates_{coordinate_precision}dp, coordinate_priority_match",
                'city_match': not ignore_city,
                'state_match': not ignore_state,
                'geographic_warning': ''
            }
            
            all_matches.append(match_info)
            matched_piggy_indices.add(piggy_idx)
            matched_ctx_indices.add(ctx_idx)
        
        self.log_message(f"Found {len(exact_coordinate_matches)} truncated coordinate matches")
        
        # STEP 2: PROXIMITY MATCHING for remaining locations (only if needed)
        remaining_piggy = len(piggy_df) - len(matched_piggy_indices)
        remaining_ctx = len(ctx_df) - len(matched_ctx_indices)
        
        if remaining_piggy > 0 and remaining_ctx > 0:
            self.log_message(f"Step 2: Processing {remaining_piggy} remaining Piggy locations")
            
            # Create filtered datasets
            unmatched_piggy_mask = ~piggy_df.index.isin(matched_piggy_indices)
            unmatched_ctx_mask = ~ctx_df.index.isin(matched_ctx_indices)
            
            remaining_piggy_df = piggy_df[unmatched_piggy_mask]
            remaining_ctx_df = ctx_df[unmatched_ctx_mask]
            
            # Process remaining locations with strict coordinate priority
            for i, piggy_row in remaining_piggy_df.iterrows():
                if pd.isna(piggy_row['latitude']) or pd.isna(piggy_row['longitude']):
                    continue
                
                # Very restrictive spatial filtering for coordinate priority
                nearby_ctx = spatial_index_filter(piggy_row['latitude'], piggy_row['longitude'], 
                                                remaining_ctx_df, max_distance)
                
                if len(nearby_ctx) == 0:
                    continue
                
                # Calculate distances
                distances = haversine_vectorized(
                    piggy_row['latitude'], piggy_row['longitude'],
                    nearby_ctx['latitude'].values, nearby_ctx['longitude'].values
                )
                
                # Filter by distance
                distance_mask = distances <= max_distance
                if not np.any(distance_mask):
                    continue
                
                candidates = nearby_ctx[distance_mask].copy()
                candidate_distances = distances[distance_mask]
                
                location_matches = []
                
                for idx, (ctx_idx, ctx_row) in enumerate(candidates.iterrows()):
                    distance = candidate_distances[idx]
                    
                    # Name similarity (if not ignored)
                    name_sim = 0.0 if ignore_name else advanced_name_similarity_cached(piggy_row['name'], ctx_row['name'])
                    
                    # Apply name similarity threshold (if not ignored)
                    if not ignore_name and name_sim < min_name_sim:
                        continue
                    
                    # Street address similarity (only when ignoring geographic components)
                    street_addr_sim = 0.0
                    if include_address and (ignore_city or ignore_state or ignore_zip):
                        street_addr_sim = street_address_similarity(piggy_row['address1'], ctx_row['address1'])
                    
                    # Calculate confidence with coordinate priority
                    confidence = calculate_confidence_score_new(
                        distance, name_sim, street_addr_sim, piggy_row, ctx_row,
                        ignore_name, ignore_city, ignore_state, ignore_zip
                    )
                    
                    if confidence < min_confidence:
                        continue
                    
                    # Create match record
                    match_info = {
                        'piggy_index': i,
                        'ctx_index': ctx_idx,
                        'distance_miles': distance,
                        'name_similarity': name_sim,
                        'address_similarity': street_addr_sim,
                        'confidence': confidence,
                        'reasons': f"coordinate_priority_proximity, distance_{distance:.3f}mi",
                        'city_match': not ignore_city,
                        'state_match': not ignore_state,
                        'geographic_warning': ''
                    }
                    
                    location_matches.append(match_info)
                
                # Sort by confidence and add matches
                if location_matches:
                    location_matches.sort(key=lambda x: x['confidence'], reverse=True)
                    
                    if show_all_matches:
                        all_matches.extend(location_matches)
                    else:
                        all_matches.append(location_matches[0])
        
        self.log_message(f"Total matches found: {len(all_matches)}")
        return all_matches
    
    def on_closing(self):
        """Handle window closing"""
        self.save_settings()
        self.root.destroy()

def main():
    root = tk.Tk()
    app = MerchantComparisonGUI(root)
    root.mainloop()

if __name__ == "__main__":
    # Handle missing packages gracefully
    missing_packages = []
    
    try:
        import pandas as pd
    except ImportError:
        missing_packages.append("pandas")
    
    try:
        import geopy
    except ImportError:
        missing_packages.append("geopy")
    
    if missing_packages:
        root = tk.Tk()
        root.withdraw()
        messagebox.showerror("Missing Dependencies", 
                           f"The following packages need to be installed:\n{', '.join(missing_packages)}\n\n"
                           "Please install them using:\npip install " + " ".join(missing_packages))
        sys.exit(1)
    
    main()