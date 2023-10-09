import React, { useState } from 'react';
import axiosInstance from '../../api/axiosInstance';
import LayoutCenter from '../LayoutCenter';
import { useNavigate, Link } from 'react-router-dom';


import {
  Card,
  Input,
  // Checkbox,
  Button,
  Typography,
} from "@material-tailwind/react";


function Login() {
		const [errorMessage, setErrorMessage] = useState(null);
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');

		const navigate = useNavigate();

    const handleLogin = async (event) => {
			event.preventDefault();
        try {
            const response = await axiosInstance.post("/user/login", { username, password });
            localStorage.setItem("token", response.data.access_token);
            navigate("/jobs");
        } catch (error) {
            console.error("Error logging in", error.response.data);
						setErrorMessage('Invalid login credentials. Please try again.');
						setUsername('');
						setPassword('');
        }
    };

    return (
				<LayoutCenter>
					<Card color="transparent" shadow={false}>
						<Typography variant="h4" color="blue-gray">
							Login
						</Typography>
						<Typography color="gray" className="mt-1 font-normal">
							Enter your details to login.
						</Typography>
						<form className="mt-8 mb-2 w-80 max-w-screen-lg sm:w-96">
							<div className="mb-4 flex flex-col gap-6">
								<Input size="lg" label="Username" value={username} onChange={(e) => setUsername(e.target.value)} />
								<Input type="password" size="lg" label="Password" value={password} onChange={(e) => setPassword(e.target.value)} />
							</div>
							<Button className="mt-6" fullWidth onClick={handleLogin}>
								Login
							</Button>
							<Typography color="gray" className="mt-4 text-center font-normal">
								No account yet?{" "}
								<Link to="/signup" className="font-medium text-gray-900">
									Create one.
								</Link>
							</Typography>
						</form>
					</Card>
				</LayoutCenter>
    );
};

export default Login;