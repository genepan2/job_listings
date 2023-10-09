import React from 'react';
import { Navigate } from 'react-router-dom';

const ProtectedRoute = ({ children }) => {
    const token = localStorage.getItem("token");

    if (!token) {
        // If there's no token, redirect to login
        return <Navigate to="/login" replace />;
    }

    return children;  // If there's a token, render the protected component
};

export default ProtectedRoute;


// import React from 'react';
// import { Outlet, useNavigate } from 'react-router-dom';

// const ProtectedRoute = ({ component: Component, ...rest }) => {
//     const token = localStorage.getItem("token");
//     const navigate = useNavigate();

//     if (!token) {
//         navigate("/login");
//         return null; // Optionally, you can return a redirect or another component here.
//     }

//     return <Outlet />;
// };

// export default ProtectedRoute;
