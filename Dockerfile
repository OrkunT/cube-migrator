# Use an official Node.js runtime as a parent image
FROM node:14

# Set the working directory in the container to /app
WORKDIR /app

# Copy package.json and package-lock.json into the working directory
COPY package*.json ./

# Install any needed packages specified in package.json
RUN npm install

# Bundle app source inside Docker image
COPY . .

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Run index.js when the container launches
CMD ["node", "mongodb/migrate.js"]
