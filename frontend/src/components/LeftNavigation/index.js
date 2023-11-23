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
import {
  PresentationChartBarIcon,
  ShoppingBagIcon,
  UserCircleIcon,
  Cog6ToothIcon,
  InboxIcon,
  PowerIcon,
  MapPinIcon,
  TrophyIcon,
  LanguageIcon,
} from "@heroicons/react/24/solid";
import { ChevronRightIcon, ChevronDownIcon } from "@heroicons/react/24/outline";

import { Checkbox } from "@material-tailwind/react";

function MultiLevelSidebar({ setFilters, stats }) {
  const [open, setOpen] = React.useState(0);

  const handleOpen = (value) => {
    setOpen(open === value ? 0 : value);
  };

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

  const locations = ["Berlin",   "Munich",   "Hamburg",   "Cologne", "Frankfurt", "Other"];
  const levels = ["Internship",   "Entry",   "Middle",   "Senior",   "Other"]
  const languages = ["English", "German"]

  const filterConfig = [
    {
      title: 'Location',
      icon: <MapPinIcon className="w-5 h-5" />,
      identifier: 'locations',
    },
    {
      title: 'Level',
      icon: <TrophyIcon className="w-5 h-5" />,
      identifier: 'levels',
    },
    {
      title: 'Language',
      icon: <LanguageIcon className="w-5 h-5" />,
      identifier: 'language',
    },
    // ... other filters
  ];

  return (
    <Card className="h-[calc(100vh-2rem)] w-full p-4" shadow={false}>
      <div className="p-4 mb-2">
        <Typography variant="h5" color="blue-gray">
          Filter
        </Typography>
      </div>
      <List>
      {/* {filterConfig.map((filter, filterIndex) => (
        <Accordion
          open={open === filterIndex}
          icon={
            <ChevronDownIcon
              strokeWidth={2.5}
              className={`mx-auto h-4 w-4 transition-transform ${open === filterIndex ? "rotate-180" : ""}`}
            />
          }
        >
          <ListItem className="p-0" selected={open === filterIndex}>
            <AccordionHeader onClick={() => handleOpen(filterIndex)} className="p-3 border-b-0">
              <ListItemPrefix>
                {filter.icon}
              </ListItemPrefix>
              <Typography color="blue-gray" className="mr-auto font-normal">
                {filter.title}
              </Typography>
            </AccordionHeader>
          </ListItem>
          <AccordionBody className="py-1">
            <List className="gap-0 p-0">
              {Object.keys(stats[filter.identifier]).map((option, index) => (
                <ListItem key={index}>
                  <Checkbox
                    label={option}
                    value={option}
                    containerProps={{
                      className: "p-0 px-3",
                    }}
                    className="p-0"
                    onChange={(e) => handleFilterChange(filter.identifier, e.target.value)}
                  />
                </ListItem>
              ))}
            </List>
          </AccordionBody>
        </Accordion>
      ))} */}

        <Accordion
          open={open === 1}
          icon={
            <ChevronDownIcon
              strokeWidth={2.5}
              className={`mx-auto h-4 w-4 transition-transform ${open === 1 ? "rotate-180" : ""}`}
            />
          }
        >
          <ListItem className="p-0" selected={open === 1}>
            <AccordionHeader onClick={() => handleOpen(1)} className="p-3 border-b-0">
              <ListItemPrefix>
                <MapPinIcon className="w-5 h-5" />
              </ListItemPrefix>
              <Typography color="blue-gray" className="mr-auto font-normal">
                Location
              </Typography>
            </AccordionHeader>
          </ListItem>
          <AccordionBody className="py-1">
            <List className="gap-0 p-0">
              {
              locations.map((location, index) => (
                <ListItem key={index}>
                  <Checkbox
                    label={
                      location +
                      (
                        stats && stats["locations"] && stats["locations"][location]
                          ? ` (${stats["locations"][location]})`
                          : " (0)"
                      )
                    }
                    value={location}
                    containerProps={{
                      className: "p-0 px-3",
                    }}
                    className="p-0"
                    onChange={(e) => handleFilterChange('location', e.target.value)}
                  />
                </ListItem>
              ))
              }
            </List>
          </AccordionBody>
        </Accordion>
        <Accordion
          open={open === 2}
          icon={
            <ChevronDownIcon
              strokeWidth={2.5}
              className={`mx-auto h-4 w-4 transition-transform ${open === 2 ? "rotate-180" : ""}`}
            />
          }
        >
          <ListItem className="p-0" selected={open === 2}>
            <AccordionHeader onClick={() => handleOpen(2)} className="p-3 border-b-0">
              <ListItemPrefix>
                <TrophyIcon className="w-5 h-5" />
              </ListItemPrefix>
              <Typography color="blue-gray" className="mr-auto font-normal">
                Level
              </Typography>
            </AccordionHeader>
          </ListItem>
          <AccordionBody className="py-1">
            <List className="p-0">
              {
              levels.map((level, index) => (
                <ListItem>
                  <Checkbox
                    label={
                      level +
                      (
                        stats && stats["levels"] && stats["levels"][level]
                          ? ` (${stats["levels"][level]})`
                          : " (0)"
                      )
                    }
                    value={level}
                    containerProps={{
                      className: "p-0 px-3",
                    }}
                    className="p-0"
                    onChange={(e) => handleFilterChange('level', e.target.value)}
                  />
                </ListItem>
              ))
              }
            </List>
          </AccordionBody>
        </Accordion>
        <Accordion
          open={open === 3}
          icon={
            <ChevronDownIcon
              strokeWidth={2.5}
              className={`mx-auto h-4 w-4 transition-transform ${open === 3 ? "rotate-180" : ""}`}
            />
          }
        >
          <ListItem className="p-0" selected={open === 3}>
            <AccordionHeader onClick={() => handleOpen(3)} className="p-3 border-b-0">
              <ListItemPrefix>
                <LanguageIcon className="w-5 h-5" />
              </ListItemPrefix>
              <Typography color="blue-gray" className="mr-auto font-normal">
                Language
              </Typography>
            </AccordionHeader>
          </ListItem>
          <AccordionBody className="py-1">
            <List className="p-0">
              {
              languages.map((language, index) => (
                <ListItem>
                  <Checkbox
                    label={
                      language +
                      (
                        stats && stats["languages"] && stats["languages"][language]
                          ? ` (${stats["languages"][language]})`
                          : " (0)"
                      )
                    }
                    value={language}
                    containerProps={{
                      className: "p-0 px-3",
                    }}
                    className="p-0"
                    onChange={(e) => handleFilterChange('language', e.target.value)}
                  />
                </ListItem>
              ))
              }
            </List>
          </AccordionBody>
        </Accordion>
        {/* <ListItem>
          <ListItemPrefix>
            <InboxIcon className="w-5 h-5" />
          </ListItemPrefix>
          Inbox
          <ListItemSuffix>
            <Chip value="14" size="sm" variant="ghost" color="blue-gray" className="rounded-full" />
          </ListItemSuffix>
        </ListItem> */}
      </List>
    </Card>
  );
}


export default MultiLevelSidebar;
