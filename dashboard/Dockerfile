FROM node:16-alpine as build
WORKDIR /app
ENV PATH /app/node_modules/.bin:$PATH
ENV REACT_APP_METRICS_IP_ADDRESS=my.featureform.com/prometheus
COPY package.json /app/package.json
COPY package-lock.json /app/package-lock.json
RUN npm install
RUN npm install -g serve
COPY . /app
RUN npm run build-docker
EXPOSE 3000
CMD ["serve", "-s", "build", "-l", "3000"]