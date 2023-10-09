import React, { useState } from 'react';
import axiosInstance from '../../api/axiosInstance';
import LayoutCenter from '../LayoutCenter';
import { useNavigate } from 'react-router-dom';

import {
  Card,
  Input,
  Checkbox,
  Button,
  Typography,
} from "@material-tailwind/react";


function Signup() {
		const [errorMessage, setErrorMessage] = useState(null);
    const [username, setUsername] = useState('');
    const [password, setPassword] = useState('');
    const [isAdmin, setIsAdmin] = useState(false);

		const navigate = useNavigate();

    const handleSignup = async (event) => {
    // const handleSignup = (event) => {
			event.preventDefault();
			try {
					// const response = axiosInstance.post("/user/signup", { username, password, isAdmin });
					const response = await axiosInstance.post("/user/signup", { username, password, isAdmin });
					localStorage.setItem("token", response.data.access_token);
					console.log(response.data.access_token)
					navigate("/jobs");
					// navigate("/dashboard");
			} catch (error) {
					console.error("Error signing up", error.response.data);
					setErrorMessage('Something went wrong. Please try again.');
			}
	};

    return (
			<LayoutCenter>
				<Card color="transparent" shadow={false}>
					<Typography variant="h4" color="blue-gray">
						Sign Up
					</Typography>
					<Typography color="gray" className="mt-1 font-normal">
						Enter your details to sigup.
					</Typography>
					<form className="mt-8 mb-2 w-80 max-w-screen-lg sm:w-96">
						<div className="mb-4 flex flex-col gap-6">
							<Input size="lg" label="Username" value={username} onChange={(e) => setUsername(e.target.value)} />
							<Input type="password" size="lg" label="Password" value={password} onChange={(e) => setPassword(e.target.value)} />
						</div>
						<Checkbox
							label={
								<Typography
									variant="small"
									color="gray"
									className="flex items-center font-normal"
								>
									Make me Admin!
								</Typography>
							}
							containerProps={{ className: "-ml-2.5" }}
							value={isAdmin}
							onChange={(e) => setIsAdmin(e.target.checked)}
						/>
						<Button className="mt-6" fullWidth onClick={handleSignup}>
							Sign Up
						</Button>
					</form>
				</Card>
			</LayoutCenter>
    );
};

export default Signup;