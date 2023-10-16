import axios from 'axios';

const baseURL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

// function createAxiosInstance(navigate) {
//     const axiosInstance = axios.create({
//         baseURL: 'https://127.0.0.1:8000',
//         headers: {
//             "Content-type": "application/json",
//             // "Authorization": `Bearer ${localStorage.getItem("token")}`
//         }
//     });

//     axiosInstance.interceptors.request.use(
//         config => {
//             const token = localStorage.getItem('token');
//             if (token) {
//                 config.headers['Authorization'] = `Bearer ${token}`;
//             }
//             return config;
//         },
//         error => {
//             return Promise.reject(error);
//         }
//     );

//     axiosInstance.interceptors.response.use(
//         response => response,
//         error => {
//             if (error.response && (error.response.status === 401 || error.response.status === 403)) {
//                 navigate('/login');
//             }
//             return Promise.reject(error);
//         }
//     );

//     return axiosInstance;
// }

// export default createAxiosInstance;


const axiosInstance = axios.create({
    // baseURL: 'http://api:8000',
    baseURL: baseURL,
    // baseURL: 'https://127.0.0.1:8000',
    headers: {
        "Content-type": "application/json",
    },
});


// // Add a request interceptor
// axiosInstance.interceptors.request.use(
//     config => {
//         const token = localStorage.getItem('token');
//         if (token) {
//             config.headers['Authorization'] = `Bearer ${token}`;
//         }
//         return config;
//     },
//     error => {
//         return Promise.reject(error);
//     }
// );

// // Add a response interceptor (Optional but recommended)
// axiosInstance.interceptors.response.use(
//     response => response,
//     error => {
//         if (error.response && (error.response.status === 401 || error.response.status === 403)) {  // Unauthorized
//             localStorage.removeItem('token');
//             console.log("Unauthorized");
//             // Handle the error, maybe redirect to login, etc.
//             // For example:
//             // history.push('/login');
//             // navigate("/login")
//             if (!window.location.href.includes('/login')) {
//                 window.location.href = '/login';
//             }

//         }
//         return Promise.reject(error);
//     }
// );

export default axiosInstance;