import React from "react";
import {
  Card,
  Typography,
  List,
  ListItem,
  ListItemPrefix,
  ListItemSuffix,
  Chip,
  Accordion,
  AccordionHeader,
  AccordionBody,
} from "@material-tailwind/react";
// import {
//   PresentationChartBarIcon,
//   ShoppingBagIcon,
//   UserCircleIcon,
//   Cog6ToothIcon,
//   InboxIcon,
//   PowerIcon,
// } from "@heroicons/react/24/solid";
// import { ChevronRightIcon, ChevronDownIcon } from "@heroicons/react/24/outline";
import { Checkbox } from "@material-tailwind/react";

function MultiLevelSidebar({ setFilters }) {
  // const [open, setOpen] = React.useState(0);

  // const handleOpen = (value) => {
  //   setOpen(open === value ? 0 : value);
  // };

  const handleFilterChange = (filterType, value) => {
    setFilters(prevFilters => {
      let updatedValues = [...prevFilters[filterType]];

      if (updatedValues.includes(value)) {
          updatedValues = updatedValues.filter(loc => loc !== value);
      } else {
          updatedValues.push(value);
      }

      return {
          ...prevFilters,
          [filterType]: updatedValues
      };
    });
  };

  return (

      <Card className="h-[calc(100vh-2rem)] w-full max-w-[20rem] p-4 text-left" shadow={false}>
        <div className="mb-2 p-4">
          <Typography variant="h5" color="blue-gray">
            Filter
          </Typography>
        </div>
        <div>
          <div className="flex w-full"><Checkbox label="Berlin" value="Berlin" onChange={(e) => handleFilterChange('location', e.target.value)} /></div>
          <div className="flex w-full"><Checkbox label="Munich" value="Munich" onChange={(e) => handleFilterChange('location', e.target.value)} /></div>
          <div className="flex w-full"><Checkbox label="Hamburg" value="Hamburg" onChange={(e) => handleFilterChange('location', e.target.value)} /></div>
          <div className="flex w-full"><Checkbox label="Cologne" value="Cologne" onChange={(e) => handleFilterChange('location', e.target.value)} /></div>
        </div>
        <div>
          <div className="flex w-full"><Checkbox label="Internship" value="Internship" onChange={(e) => handleFilterChange('level', e.target.value)} /></div>
          <div className="flex w-full"><Checkbox label="Entry" value="Entry" onChange={(e) => handleFilterChange('level', e.target.value)} /></div>
          <div className="flex w-full"><Checkbox label="Middle" value="Middle" onChange={(e) => handleFilterChange('level', e.target.value)} /></div>
          <div className="flex w-full"><Checkbox label="Senior" value="Senior" onChange={(e) => handleFilterChange('level', e.target.value)} /></div>
          <div className="flex w-full"><Checkbox label="Other" value="Other" onChange={(e) => handleFilterChange('level', e.target.value)} /></div>
        </div>
      </Card>

  );
}

export default MultiLevelSidebar;

// function LeftNavigation({ setFilters }) {
//   const handleFilterChange = (filterType, value) => {
//       setFilters(prevFilters => ({
//           ...prevFilters,
//           [filterType]: value
//       }));
//   };

//   return (
//       <div className="left-nav">
//           {/* <div className="filter-container">
//               <p>Source</p>
//               <label>
//                   <input type="checkbox" value="themuse" onChange={(e) => handleFilterChange('source', e.target.value)} /> TheMuse
//               </label>
//               <label>
//                   <input type="checkbox" value="whatjobs" onChange={(e) => handleFilterChange('source', e.target.value)} /> WhatJobs
//               </label>
//               <label>
//                   <input type="checkbox" value="linkedin" onChange={(e) => handleFilterChange('source', e.target.value)} /> linkedIn
//               </label>
//           </div> */}

//           <div className="filter-container">
//               <p>Location</p>
//               <label>
//                   <input type="checkbox" value="Berlin" onChange={(e) => handleFilterChange('location', e.target.value)} /> Berlin
//               </label>
//               <label>
//                   <input type="checkbox" value="Munich" onChange={(e) => handleFilterChange('location', e.target.value)} /> Munich
//               </label>
//               <label>
//                   <input type="checkbox" value="Hamburg" onChange={(e) => handleFilterChange('location', e.target.value)} /> Hamburg
//               </label>
//               <label>
//                   <input type="checkbox" value="Cologne" onChange={(e) => handleFilterChange('location', e.target.value)} /> Cologne
//               </label>
//           </div>

//           <div className="filter-container">
//               <p>Job Level</p>
//               <label>
//                   <input type="checkbox" value="Intern" onChange={(e) => handleFilterChange('level', e.target.value)} /> Internship
//               </label>
//               <label>
//                   <input type="checkbox" value="Entry" onChange={(e) => handleFilterChange('level', e.target.value)} /> Entry
//               </label>
//               <label>
//                   <input type="checkbox" value="Mid" onChange={(e) => handleFilterChange('level', e.target.value)} /> Mid
//               </label>
//               <label>
//                   <input type="checkbox" value="Senior" onChange={(e) => handleFilterChange('level', e.target.value)} /> Senior
//               </label>
//               <label>
//                   <input type="checkbox" value="Unknown" onChange={(e) => handleFilterChange('level', e.target.value)} /> Unknown
//               </label>
//           </div>

//           {/* ... rest of the code ... */}
//       </div>
//   );
// }


// export default LeftNavigation;