import React from 'react';
import JobCard from './JobCard';

function MainArea({ jobs }) {
    return (
        <div className="main-area">
            {jobs.map(job => <JobCard key={job.id} job={job} />)}
            <div className="pagination">
                {/* Placeholder for pagination */}
            </div>
        </div>
    );
}

export default MainArea;
