function LeftNavigation({ setFilters }) {
  const handleFilterChange = (filterType, value) => {
      setFilters(prevFilters => ({
          ...prevFilters,
          [filterType]: value
      }));
  };

  return (
      <div className="left-nav">
          {/* <div className="filter-container">
              <p>Source</p>
              <label>
                  <input type="checkbox" value="themuse" onChange={(e) => handleFilterChange('source', e.target.value)} /> TheMuse
              </label>
              <label>
                  <input type="checkbox" value="whatjobs" onChange={(e) => handleFilterChange('source', e.target.value)} /> WhatJobs
              </label>
              <label>
                  <input type="checkbox" value="linkedin" onChange={(e) => handleFilterChange('source', e.target.value)} /> linkedIn
              </label>
          </div> */}

          <div className="filter-container">
              <p>Location</p>
              <label>
                  <input type="checkbox" value="Berlin" onChange={(e) => handleFilterChange('location', e.target.value)} /> Berlin
              </label>
              <label>
                  <input type="checkbox" value="Munich" onChange={(e) => handleFilterChange('location', e.target.value)} /> Munich
              </label>
              <label>
                  <input type="checkbox" value="Hamburg" onChange={(e) => handleFilterChange('location', e.target.value)} /> Hamburg
              </label>
              <label>
                  <input type="checkbox" value="Cologne" onChange={(e) => handleFilterChange('location', e.target.value)} /> Cologne
              </label>
          </div>

          <div className="filter-container">
              <p>Job Level</p>
              <label>
                  <input type="checkbox" value="Intern" onChange={(e) => handleFilterChange('level', e.target.value)} /> Internship
              </label>
              <label>
                  <input type="checkbox" value="Entry" onChange={(e) => handleFilterChange('level', e.target.value)} /> Entry
              </label>
              <label>
                  <input type="checkbox" value="Mid" onChange={(e) => handleFilterChange('level', e.target.value)} /> Mid
              </label>
              <label>
                  <input type="checkbox" value="Senior" onChange={(e) => handleFilterChange('level', e.target.value)} /> Senior
              </label>
              <label>
                  <input type="checkbox" value="Unknown" onChange={(e) => handleFilterChange('level', e.target.value)} /> Unknown
              </label>
          </div>

          {/* ... rest of the code ... */}
      </div>
  );
}


export default LeftNavigation;