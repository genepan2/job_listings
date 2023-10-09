import React from "react";
import MultiLevelSidebar from '../LeftNavigation';
import MainArea from '../MainArea';
import LayoutSidebar from '../LayoutSidebar';


	function JobListing({ filters, setFilters, jobs }) {
	return (
			<LayoutSidebar sidebar={<MultiLevelSidebar setFilters={setFilters} />} mainContent={<MainArea jobs={jobs} />}/>
			// <div className="container mx-auto">
			// 		<div className="flex flex-row flex-wrap py-4">
			// 				<aside className="w-full sm:w-1/3 md:w-1/4 px-2">
			// 						<div className="sticky top-0 p-4 w-full">
			// 								<MultiLevelSidebar setFilters={setFilters} />
			// 						</div>
			// 				</aside>
			// 				<MainArea jobs={jobs} />
			// 		</div>
			// </div>
	);
}

export default JobListing