# RUN docker build . -t dashboard
# docker tag dashboard:latest featureformcom/dashboard:latest
# docker push featureformcom/dashboard:latest

#FROM node:16-alpine
#COPY . .
#RUN npm install
#EXPOSE 8000
#ENTRYPOINT npm run start


FROM node:16-alpine
COPY . .
RUN yarn install
RUN yarn run build
RUN ls
RUN yarn global add serve
EXPOSE 3000
ENTRYPOINT serve -s build
#WORKDIR /app
#COPY package*.json ./
#RUN npm install --production
#COPY . .
#CMD ["npm", "start"]