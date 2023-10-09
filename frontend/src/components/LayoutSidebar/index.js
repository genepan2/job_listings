import React from 'react';
import StickyNavbar from '../StickyNavbar'

const LayoutSidebar = ({ sidebar, mainContent }) => {
	return (
		<div className="h-screen grid grid-cols-12 gap-0">
			<header className="row-auto col-span-12">
				<StickyNavbar />
			</header>
			<div className="row-auto col-span-12 grid grid-cols-12 grid-flow-col gap-0 w-full">
				<aside class="text-white col-span-3 border-1 border-r border-gray-400">
					{sidebar}
				{/* <nav class="w-56 flex">
				</nav> */}
				</aside>
				<main class="grid col-span-9">
					{mainContent}
				</main>
			</div>
		</div>
	);
}

export default LayoutSidebar;