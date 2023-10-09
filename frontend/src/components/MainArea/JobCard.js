import React from 'react';
import moment from 'moment';

import {
  Card,
  CardBody,
  CardFooter,
  Typography,
  Button,
  Chip
} from "@material-tailwind/react";
import { Link } from 'react-router-dom';

function JobCard( {job} ) {
  return (
    <Card className="mt-0 w-full text-left border-1 border-b border-gray-400 rounded-none hover:bg-gray-100" shadow={false}>
      <CardBody>
          {/* <Chip size="md" color="pink" variant="outlined" value= /> */}
        <Typography variant="h5" color="blue-gray" className="mb-2">
          ({job.level}) {job.title}
        </Typography>
        <Typography variant="small" className="mb-2 font-medium">
					@{job.company_name} / in {job.location} / {job.level}
        </Typography>
        <Typography>
					{job.description}... <span>read more</span>
        </Typography>
        <Typography>
					<Link to={job.url}>Source</Link>
        </Typography>
        <Typography>
				  Applicants: {job.applicants ? job.applicants : '??'}
        </Typography>
        <Typography>
          {moment(job.publish_date).format('MMMM D, YYYY')},  {(moment(job.publish_date).fromNow())}
        </Typography>
      </CardBody>
      {/* <CardFooter className="pt-0"> */}
        {/* <Button>Read More</Button> */}
      {/* </CardFooter> */}
    </Card>
  );
}


// function JobCard({ job }) {
//     return (
// 			<div
// 				class="block rounded-lg bg-white text-center shadow-[0_2px_15px_-3px_rgba(0,0,0,0.07),0_10px_20px_-2px_rgba(0,0,0,0.04)] dark:bg-neutral-700">
// 				<div
// 					class="border-b-2 border-neutral-100 px-6 py-3 dark:border-neutral-600 dark:text-neutral-50">
// 					Featured
// 				</div>
// 				<div class="p-6">
// 					<h5
// 						class="mb-2 text-xl font-medium leading-tight text-neutral-800 dark:text-neutral-50">
// 						{job.job_title}
// 					</h5>
// 					<p class="mb-4 text-base text-neutral-600 dark:text-neutral-200">
// 						{job.job_description.substring(0, 300)}... <span>read more</span>
// 					</p>
// 					<button
// 						type="button"
// 						href="#"
// 						class="inline-block rounded bg-primary px-6 pb-2 pt-2.5 text-xs font-medium uppercase leading-normal text-white shadow-[0_4px_9px_-4px_#3b71ca] transition duration-150 ease-in-out hover:bg-primary-600 hover:shadow-[0_8px_9px_-4px_rgba(59,113,202,0.3),0_4px_18px_0_rgba(59,113,202,0.2)] focus:bg-primary-600 focus:shadow-[0_8px_9px_-4px_rgba(59,113,202,0.3),0_4px_18px_0_rgba(59,113,202,0.2)] focus:outline-none focus:ring-0 active:bg-primary-700 active:shadow-[0_8px_9px_-4px_rgba(59,113,202,0.3),0_4px_18px_0_rgba(59,113,202,0.2)] dark:shadow-[0_4px_9px_-4px_rgba(59,113,202,0.5)] dark:hover:shadow-[0_8px_9px_-4px_rgba(59,113,202,0.2),0_4px_18px_0_rgba(59,113,202,0.1)] dark:focus:shadow-[0_8px_9px_-4px_rgba(59,113,202,0.2),0_4px_18px_0_rgba(59,113,202,0.1)] dark:active:shadow-[0_8px_9px_-4px_rgba(59,113,202,0.2),0_4px_18px_0_rgba(59,113,202,0.1)]"
// 						data-te-ripple-init
// 						data-te-ripple-color="light">
// 						Go somewhere
// 					</button>
// 				</div>
// 				<div
// 					class="border-t-2 border-neutral-100 px-6 py-3 dark:border-neutral-600 dark:text-neutral-50">
// 					{job.publishDate}
// 				</div>
// 			</div>

//         // <div className="job-card">
//         //     <h2 style={{ fontSize: '30px', color: 'black' }}>{job.job_title}</h2>
//         //     <p style={{ fontSize: '16px', color: 'grey' }}>{job.company_name}, {job.job_location}, {job.category}, {job.job_level}, {job.salary}</p>
//         //     <p style={{ fontSize: '21px', color: 'black' }}>{job.job_description.substring(0, 300)}... <span>read more</span></p>
//         //     <p style={{ fontSize: '16px', color: 'grey' }}>{job.source}, {job.publishDate}, {job.applicants} applicants</p>
//         // </div>
//     );
// }

export default JobCard;
