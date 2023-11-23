import React from 'react';
// import JobCard from './JobCard';
import moment from 'moment';
// import TableRow from './TableRow';


import { PencilIcon } from "@heroicons/react/24/solid";
import {
  ArrowDownTrayIcon,
  MagnifyingGlassIcon,
} from "@heroicons/react/24/outline";
import {
  Card,
  CardHeader,
  Typography,
  Button,
  CardBody,
  Chip,
  CardFooter,
  Avatar,
  IconButton,
  Tooltip,
  Input,
} from "@material-tailwind/react";

const LEVELS = {
    Intern: "blue",
    Entry: "red",
    Middle: "green",
    Senior: "amber",
    Unknown: "pink",
    Lead: "indigo",
    Head: "purple",
    Student: "teal",
}

const LANG = {
    English: "EN",
    German: "DE"
}
const TABLE_HEAD = ["Level", "Title", "Location (Lang)", "Date", "Salary", ""];


function MainArea({ jobs }) {
    return (
        <Card className="w-full h-full">
          {/* <CardHeader floated={false} shadow={false} className="rounded-none">
            <div className="flex flex-col justify-between gap-8 mb-4 md:flex-row md:items-center">
              <div>
                <Typography variant="h5" color="blue-gray">
                  Recent Transactions
                </Typography>
                <Typography color="gray" className="mt-1 font-normal">
                  These are details about the last transactions
                </Typography>
              </div>
              <div className="flex w-full gap-2 shrink-0 md:w-max">
                <div className="w-full md:w-72">
                  <Input
                    label="Search"
                    icon={<MagnifyingGlassIcon className="w-5 h-5" />}
                  />
                </div>
                <Button className="flex items-center gap-3" size="sm">
                  <ArrowDownTrayIcon strokeWidth={2} className="w-4 h-4" /> Download
                </Button>
              </div>
            </div>
          </CardHeader> */}
          <CardBody className="px-0 overflow-scroll">
            <table className="w-full text-left table-auto min-w-max">
              <thead>
                <tr>
                  {TABLE_HEAD.map((head) => (
                    <th
                      key={head}
                      className="p-4 border-y border-blue-gray-100 bg-blue-gray-50/50"
                    >
                      <Typography
                        variant="small"
                        color="blue-gray"
                        className="font-normal leading-none opacity-70"
                      >
                        {head}
                      </Typography>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {jobs.map(
                  (
                    {
                      title,
                      company_name,
                      location,
                      level,
                      publish_date,
                      url,
                      language,
                      predicted_salary
                    },
                    index,
                  ) => {
                    const isLast = index === jobs.length - 1;
                    const classes = isLast
                      ? "p-4"
                      : "p-4 border-b border-blue-gray-50";
                    if (!title || !level || !location) {
                      return null; // null means nothing will be rendered for this item.
                    }
                    return (
                      <tr key={index}>
                        <td className={classes}>
                          <div className="w-max">
                            <Chip
                              size="sm"
                              variant="ghost"
                              value={level}
                              color={LEVELS[level]}
                            />
                          </div>
                        </td>
                        <td className={classes}>
                          <div className="flex flex-col gap-0">
                            <Typography
                              variant="small"
                              color="blue-gray"
                              className="font-bold"
                            >
                              {title}
                            </Typography>

                            <Typography
                            variant="small"
                            color="blue-gray"
                            className="font-normal"
                          >
                            @{company_name}
                          </Typography>
                          </div>
                        </td>
                        <td className={classes}>
                        <Typography
                            variant="small"
                            color="blue-gray"
                            className="font-normal"
                          >
                            {location} {LANG[language]}
                          </Typography>
                        </td>
                        <td className={classes}>
                            <div className="flex flex-col gap-0">
                                <Typography
                                    variant="small"
                                    color="blue-gray"
                                    className="font-normal"
                                >
                                    {moment(publish_date).format('MMMM D, YYYY')}
                                </Typography>
                                <Typography
                                    variant="small"
                                    color="blue-gray"
                                    className="font-normal"
                                >
                                    {(moment(publish_date).fromNow())}
                                </Typography>
                            </div>
                        </td>
                        <td className={classes}>
                          <div className="flex items-center gap-3">
                            <div className="flex flex-col">
                              <Typography
                                variant="small"
                                color="blue-gray"
                                className="font-normal capitalize"
                              >
                                {predicted_salary}
                              </Typography>
                            </div>
                          </div>
                        </td>
                        <td className={classes}>
                          {/* <Tooltip content="Edit User">
                            <IconButton variant="text">
                              <PencilIcon className="w-4 h-4" />
                            </IconButton>
                          </Tooltip> */}
                            <a href={url} rel="noreferrer" target="_blank">
                                <Button
                                    size="sm"
                                    variant="outlined"
                                >
                                    Source
                                </Button>
                            </a>
                        </td>
                      </tr>
                    );
                  },
                )}
              </tbody>
            </table>
          </CardBody>
          {/* <CardFooter className="flex items-center justify-between p-4 border-t border-blue-gray-50">
            <Button variant="outlined" size="sm">
              Previous
            </Button>
            <div className="flex items-center gap-2">
              <IconButton variant="outlined" size="sm">
                1
              </IconButton>
              <IconButton variant="text" size="sm">
                2
              </IconButton>
              <IconButton variant="text" size="sm">
                3
              </IconButton>
              <IconButton variant="text" size="sm">
                ...
              </IconButton>
              <IconButton variant="text" size="sm">
                8
              </IconButton>
              <IconButton variant="text" size="sm">
                9
              </IconButton>
              <IconButton variant="text" size="sm">
                10
              </IconButton>
            </div>
            <Button variant="outlined" size="sm">
              Next
            </Button>
          </CardFooter> */}
        </Card>
    );
}



// function MainArea({ jobs }) {
//     return (
// 		// <main role="main" className="w-full px-2 pt-1 sm:w-2/3 md:w-3/4">
// 		<div className="w-full">
//             {jobs.map((job, index) => <JobCard key={index} job={job} />)}
//             <div className="pagination">
//                 {/* Placeholder for pagination */}
//             </div>
//         </div>
//     );
// }

export default MainArea;
