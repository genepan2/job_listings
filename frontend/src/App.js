import React, { useState, useEffect } from 'react';
// import axios from 'axios';  // Install axios with npm install axios
import axiosInstance from './api/axiosInstance';

import LeftNavigation from './components/LeftNavigation';
import MainArea from './components/MainArea';

import logo from './logo.svg';
import './App.css';

function App() {
  const [jobs, setJobs] = useState([]);
  const [filters, setFilters] = useState({
      keyword: '',
      level: '',
      location: '',
      age: 1,
      order: 'asc',
      page: 1,
      items_per_page: 10
  });

  useEffect(() => {
      const fetchJobs = async () => {
          // console.log(filters)
          try {
              const response = await axiosInstance.post('/jobs', filters);
              console.log(response)
              setJobs(response.data.data);
          } catch (error) {
              console.error("Error fetching jobs:", error);
          }
      };

      fetchJobs();
  }, [filters]);

  return (
      <div className="App">
          <LeftNavigation setFilters={setFilters} />
          <MainArea jobs={jobs} />
      </div>
  );
}


// function App() {
//   return (
//     <div className="App">
//       <header className="App-header">
//         <img src={logo} className="App-logo" alt="logo" />
//         <p>
//           Edit <code>src/App.js</code> and save to reload.
//         </p>
//         <a
//           className="App-link"
//           href="https://reactjs.org"
//           target="_blank"
//           rel="noopener noreferrer"
//         >
//           Learn React
//         </a>
//       </header>
//     </div>
//   );
// }

export default App;
