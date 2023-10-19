import React from 'react';
import StickyNavbar from '../StickyNavbar'

const LayoutSidebar = ({ sidebar, mainContent }) => {
	return (
		<div className="grid h-screen grid-cols-12 gap-0">
			<header className="col-span-12 row-auto">
				<StickyNavbar />
			</header>
			<div className="grid w-full grid-flow-col grid-cols-12 col-span-12 row-auto gap-0">
				<aside className="col-span-3 text-white border-r border-gray-400 border-1">
					{sidebar}
				{/* <nav className="flex w-56">
				</nav> */}
				</aside>
				<main className="grid col-span-9">
					{mainContent}
				</main>
			</div>
		</div>
	);
}

export default LayoutSidebar;