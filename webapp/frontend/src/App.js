import React, { useState } from 'react';
import { BrowserRouter as Router, Route, Routes, useNavigate, Link } from 'react-router-dom';
import './App.css';
import HomePage from './components/HomePage';
import SearchResults from './components/SearchResults';
import logo from './logo.png';

function App() {
  return (
    <Router>
      <div className="App">
        <header className="App-header">
          <div className="top-banner">
            <p>Free shipping on orders over $200!</p>
          </div>
          <Navbar />
        </header>
        <div className="main-content">
          <aside className="sidebar">
            <ul>
              <li><Link to="/">Home</Link></li>
              <li><Link to="#">Outdoor Furniture</Link></li>
              <li><Link to="#">Outdoor Power Equipment</Link></li>
              <li><Link to="#">Tools</Link></li>
              <li><Link to="#">Sneakers</Link></li>
              <li><Link to="#">New</Link></li>
              <li><Link to="#">Sale</Link></li>
            </ul>
          </aside>
          <main>
            <Routes>
              <Route path="/" element={<HomePage />} />
              <Route path="/search" element={<SearchResults />} />
            </Routes>
          </main>
        </div>
        <Footer />
      </div>
    </Router>
  );
}

const Navbar = () => {
  const [query, setQuery] = useState('');
  const navigate = useNavigate();

  const handleSearch = () => {
    if (query.trim() !== '') {
      navigate(`/search?query=${query}`);
    }
  };

  return (
    <div className="navbar">
      <Link to="/" className="logo-container">
        <img src={logo} alt="Logo" className="logo-image" />
        <div className="logo-text">Astriate Gear</div>
      </Link>
      <div className="search-bar">
        <input
          type="text"
          placeholder="Search for products..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        <button onClick={handleSearch}>Search</button>
      </div>
    </div>
  );
};

const Footer = () => (
  <footer className="App-footer">
    <div className="footer-content">
      <div className="footer-section">
        <h4>About Us</h4>
        <p>Why ever even be inside? Create your garden paradise, between flowers and trees!</p>
      </div>
      <div className="footer-section">
        <h4>Customer Service</h4>
        <ul>
          <li><Link to="#">Contact Us</Link></li>
          <li><Link to="#">Shipping & Returns</Link></li>
          <li><Link to="#">FAQ</Link></li>
        </ul>
      </div>
      <div className="footer-section">
        <h4>Follow Us</h4>
        <ul className="social-media">
          <li><Link to="#">Facebook</Link></li>
          <li><Link to="#">Twitter</Link></li>
          <li><Link to="#">Instagram</Link></li>
        </ul>
      </div>
    </div>
  </footer>
);

export default App;
