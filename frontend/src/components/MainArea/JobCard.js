import React from 'react';

function JobCard({ job }) {
    return (
        <div className="job-card">
            <h2 style={{ fontSize: '30px', color: 'black' }}>{job.job_title}</h2>
            <p style={{ fontSize: '16px', color: 'grey' }}>{job.company_name}, {job.job_location}, {job.category}, {job.job_level}, {job.salary}</p>
            <p style={{ fontSize: '21px', color: 'black' }}>{job.job_description.substring(0, 300)}... <span>read more</span></p>
            <p style={{ fontSize: '16px', color: 'grey' }}>{job.source}, {job.publishDate}, {job.applicants} applicants</p>
        </div>
    );
}

export default JobCard;
