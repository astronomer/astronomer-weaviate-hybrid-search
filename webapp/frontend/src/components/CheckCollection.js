import React, { useState } from 'react';
import axios from 'axios';

function CheckCollection() {
  const [collectionName, setCollectionName] = useState('');
  const [result, setResult] = useState(null);

  const handleCheck = async () => {
    try {
      const response = await axios.post('http://localhost:5000/check-collection', { collection_name: collectionName });
      setResult(response.data.exists ? 'Collection exists' : 'Collection does not exist');
    } catch (error) {
      setResult(`Error: ${error.response ? error.response.data.error : error.message}`);
    }
  };

  return (
    <div>
      <h2>Check Collection</h2>
      <input
        type="text"
        value={collectionName}
        onChange={(e) => setCollectionName(e.target.value)}
        placeholder="Enter collection name"
      />
      <button onClick={handleCheck}>Check</button>
      {result && <p>{result}</p>}
    </div>
  );
}

export default CheckCollection;
