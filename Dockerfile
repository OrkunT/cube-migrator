# Use an official Node.js runtime as the base image
FROM node:14

# Set the working directory in the container to /app
WORKDIR /app

# Install git
RUN apt-get update && apt-get install -y git

# Clone the first GitHub repository
RUN git clone https://github.com/OrkunT/cubes.git ./cubes

# Install the dependencies in the container for the first repository
WORKDIR /app/cubes
RUN npm install

# Clone the second GitHub repository
WORKDIR /app
RUN git clone https://github.com/OrkunT/cube-migrator.git ./cube-migrator

# Install the dependencies in the container for the second repository
WORKDIR /app/cube-migrator
RUN npm install

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Run index.js when the container launches
CMD ["node", "index.js"] 
