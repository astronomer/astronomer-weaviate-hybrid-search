import React from 'react';
import hero from '../hero.png';
import furniture from '../furniture.jpg';
import lawnmower from '../lawnmower.jpg';
import shoes from '../shoes.jpg';

const HomePage = () => {
  return (
    <div>
      <div className="hero-image">
        <img src={hero} alt="Hero" style={{ width: '1200px', height: '400px' }} />
      </div>
      <div className="featured-products">
        <h2>Featured Products</h2>
        <div className="feat-product-grid">
          <div className="feat-product">
            <img src={lawnmower} alt="Riding Lawn Mower" className="feat-product-image" />
            <div className="feat-product-details">
              <h3>Riding Lawn Mower</h3>
              <p>$999.99</p>
            </div>
          </div>
          <div className="feat-product">
            <img src={furniture} alt="Elegant Patio Dining Set" className="feat-product-image" />
            <div className="feat-product-details">
              <h3>Elegant Patio Dining Set</h3>
              <p>$499.99</p>
            </div>
          </div>
          <div className="feat-product">
            <img src={shoes} alt="Classic Leather Hiking Boots" className="feat-product-image" />
            <div className="feat-product-details">
              <h3>Classic Leather Hiking Boots</h3>
              <p>$129.99</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default HomePage;
