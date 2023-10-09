import React from 'react';

const LayoutCenter = ( {children} ) => {
	return (
		<div className="h-screen flex items-center justify-center">
			<div>
				{children}
			</div>
		</div>
	);
}



export default LayoutCenter;