import React, { useState } from 'react';
import axios from 'axios';
import './SearchBar.css';  // Create a CSS file for styling if needed

function SearchBar() {
  const [query, setQuery] = useState('');
  const [result, setResult] = useState('');

  const handleSearch = async () => {
    try {
      const response = await axios.post('http://localhost:5000/search', { query });
      setResult(response.data.result);
    } catch (error) {
      setResult(`Error: ${error.response ? error.response.data.error : error.message}`);
    }
  };

  return (
    <div className="SearchBar">
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Enter your query"
      />
      <button onClick={handleSearch}>Search</button>
      {result && <p>{result}</p>}
    </div>
  );
}

export default SearchBar;
