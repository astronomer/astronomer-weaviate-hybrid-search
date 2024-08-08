import React, { useState, useEffect, useRef } from 'react';
import { useLocation } from 'react-router-dom';

const SearchResults = () => {
  const [results, setResults] = useState([]);
  const [advancedSearch, setAdvancedSearch] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [numResults, setNumResults] = useState(10);
  const [category, setCategory] = useState('');
  const [alpha, setAlpha] = useState(0.5);
  const [generative, setGenerative] = useState(false);
  const location = useLocation();
  const searchInputRef = useRef(null);

  useEffect(() => {
    const query = new URLSearchParams(location.search).get('query');
    if (query) {
      setSearchQuery(query);
      searchProducts(query);
    }

    const handleKeyDown = (event) => {
      if (event.key === 'Enter') {
        handleAdvancedSearch();
      }
      if ((event.metaKey || event.ctrlKey) && event.key === 'k') {
        event.preventDefault();
        searchInputRef.current.focus();
      }
    };

    window.addEventListener('keydown', handleKeyDown);

    return () => {
      window.removeEventListener('keydown', handleKeyDown);
    };
  }, [location]);

  const searchProducts = async (query) => {
    try {
      const params = new URLSearchParams({
        query,
        numResults,
        category,
        alpha,
        generative,
      });
      const response = await fetch(`http://localhost:5000/search?${params.toString()}`);
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Error fetching search results:', error);
    }
  };

  const handleAdvancedSearch = () => {
    searchProducts(searchQuery);
  };

  const handleToggleChange = () => {
    setGenerative(prevState => !prevState);
  };

  return (
    <div>
      <div className="search-results-header">
        <h2>Search Results</h2>
        <button
          className="toggle-advanced-search"
          onClick={() => setAdvancedSearch(!advancedSearch)}
        >
          {advancedSearch ? 'Hide Advanced Search' : 'Show Advanced Search'}
        </button>
      </div>
      <div className={`advanced-search ${advancedSearch ? 'expanded' : 'collapsed'}`}>
        <div className="advanced-search-bar">
          <input
            type="text"
            placeholder="Search for products..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            ref={searchInputRef}
          />
          <button onClick={() => handleAdvancedSearch()}>Search</button>
        </div>
        <div className="advanced-search-options">
          <label>
            Max Number of Results:
            <input
              type="number"
              value={numResults}
              onChange={(e) => setNumResults(e.target.value)}
              min="1"
              max="100"
            />
          </label>
          <label>
            Category:
            <select value={category} onChange={(e) => setCategory(e.target.value)}>
              <option value="">All</option>
              <option value="sneakers">Sneakers</option>
              <option value="outdoor furniture">Outdoor Furniture</option>
              <option value="outdoor power equipment">Outdoor Power Equipment</option>
              <option value="tools">Tools</option>
            </select>
          </label>
          
          <label>
            <div className="slider-container">
              <span className="slider-label-text">I know what I want</span>
              <input
                  type="range"
                  min="0"
                  max="1"
                  step="0.01"
                  value={alpha}
                  onChange={(e) => setAlpha(e.target.value)}
                  className="slider"
              />
              <span className="slider-label-text">Looking for ideas</span>
            </div>
          </label>


        </div>
      </div>
      {results.length > 0 ? (
        <div className="product-grid">
          {results.map((product, index) => (
            <div key={index} className="product">
              <div className="product-details">
                <h3>{product.title}</h3>
                <p>{product.description}</p>
                <p>${product.price.toFixed(2)}</p>
              </div>
              <div className="product-image">
                <img src={product.file_path} alt={product.title} />
              </div>
            </div>
          ))}
        </div>
      ) : (
        <p>No results to display</p>
      )}
    </div>
  );
};

export default SearchResults;
