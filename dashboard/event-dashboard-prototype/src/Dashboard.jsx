import React, { useState } from 'react';

const Widget = ({ index }) => {
  const [color, setColor] = useState('bg-white');

  const handleColorChange = (selectedColor) => {
    if (selectedColor === 'RED') setColor('bg-red-200');
    if (selectedColor === 'YELLOW') setColor('bg-yellow-200');
    if (selectedColor === 'GREEN') setColor('bg-green-200');
  };

  return (
    <div className={`p-4 rounded-2xl shadow-md transition-colors ${color}`}>
      <div className="flex justify-between items-start mb-2">
        <h2 className="text-xl font-semibold">Event Name: Test Event {index + 1}</h2>
        <select
          className="border rounded p-1"
          onChange={(e) => handleColorChange(e.target.value)}
          value={color === 'bg-white' ? '' : color === 'bg-red-200' ? 'RED' : color === 'bg-yellow-200' ? 'YELLOW' : 'GREEN'}
        >
          <option value="">Color</option>
          <option value="RED">Red</option>
          <option value="YELLOW">Yellow</option>
          <option value="GREEN">Green</option>
        </select>
      </div>
      <p className="text-sm font-medium mb-1">Event Description:</p>
      <p className="text-sm text-gray-700 mb-3">
        Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.
      </p>
      <p className="text-sm font-medium mb-1">Time:</p>
      <p className="text-sm text-gray-700 mb-3">12:00 PM - 2:00 PM, 25th June 2025</p>
      <div className="mb-2">
        <label className="text-sm font-medium block mb-1">Impact:</label>
        <textarea placeholder="Describe impact..." className="w-full p-2 border rounded" />
      </div>
      <div className="mb-2">
        <label className="text-sm font-medium block mb-1">Impact Owner:</label>
        <input placeholder="Enter name..." className="w-full p-2 border rounded" />
      </div>
      <div>
        <label className="text-sm font-medium block mb-1">Point of Contact:</label>
        <input placeholder="Enter contact..." className="w-full p-2 border rounded" />
      </div>
    </div>
  );
};

const Dashboard = () => {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 p-4">
      {[...Array(12)].map((_, index) => (
        <Widget key={index} index={index} />
      ))}
    </div>
  );
};

export default Dashboard;
