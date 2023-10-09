import React, { useState, useEffect } from 'react';
import axiosInstance from './api/axiosInstance';
// import createAxiosInstance from './api/axiosInstance';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
// import { useNavigate } from 'react-router-dom';


import Login from './components/Login';
import Signup from './components/Signup';
import ProtectedRoute from './components/ProtectedRoute';
// import MultiLevelSidebar from './components/LeftNavigation';
// import MainArea from './components/MainArea';
import JobsListing from './components/JobsListing';

// import logo from './logo.svg';
import './App.css';

function App() {
  const [jobs, setJobs] = useState([]);
  const [filters, setFilters] = useState({
    //   keyword: '',
      level: [],
      location: [],
      age: 1,
      order: 'asc',
      page: 1,
      items_per_page: 10
  });

//   const navigate = useNavigate();
//   const axiosInstance = createAxiosInstance(navigate);

  useEffect(() => {
      const fetchJobs = async () => {
          console.log("filters")
          console.log(filters)
          try {
              const response = await axiosInstance.post('/jobs', filters);
            //   console.log("ssdfsdgf")
            //   console.log(response)
              setJobs(response.data.data);
          } catch (error) {
              console.error("Error fetching jobs:", error);
          }
      };

      fetchJobs();
  }, [filters]);

  return (
    <Router>
        <Routes>
            <Route path="/login" element={<Login />} />
            <Route path="/signup" element={<Signup />} />
            <Route path="/jobs" element={
                <ProtectedRoute>
                    <JobsListing filters={filters} setFilters={setFilters} jobs={jobs} />
                </ProtectedRoute>
            }/>
        </Routes>
    </Router>


  );
}

export default App;

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

