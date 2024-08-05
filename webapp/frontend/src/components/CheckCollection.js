import React, { useState, useEffect } from 'react';

const CheckCollection = ({ query }) => {
  const [results, setResults] = useState([]);

  useEffect(() => {
    if (query.trim() !== '') {
      searchProducts(query);
    }
  }, [query]);

  const searchProducts = async (query) => {
    try {
      const response = await fetch(`http://localhost:5000/search?query=${query}`);
      const data = await response.json();
      setResults(data);
    } catch (error) {
      console.error('Error fetching search results:', error);
    }
  };

  return (
    <div className="check-collection">
      {results.length > 0 ? (
        <div className="results">
          {results.map((product) => (
            <div key={product.id} className="product">
              <h2>{product.title}</h2>
              <p>{product.description}</p>
            </div>
          ))}
        </div>
      ) : (
        <p>No results to display</p>
      )}
    </div>
  );
};

export default CheckCollection;
