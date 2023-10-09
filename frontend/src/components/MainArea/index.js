import React from 'react';
import JobCard from './JobCard';

function MainArea({ jobs }) {
    return (
		// <main role="main" className="w-full sm:w-2/3 md:w-3/4 pt-1 px-2">
		<div className="w-full">
            {jobs.map((job, index) => <JobCard key={index} job={job} />)}
            <div className="pagination">
                {/* Placeholder for pagination */}
            </div>
        </div>
    );
}

export default MainArea;
