FROM node:16-alpine as build
WORKDIR /app
ENV PATH /app/node_modules/.bin:$PATH
COPY package.json /app/package.json
COPY package-lock.json /app/package-lock.json
RUN npm install
RUN npm install -g serve
COPY . /app
RUN npm run build
RUN react-scripts build
EXPOSE 3000
CMD ["serve", "-s", "build", "-l", "3000"]