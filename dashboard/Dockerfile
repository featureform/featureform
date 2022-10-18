# RUN docker build . -t dashboard
# docker tag dashboard:latest featureformcom/dashboard:latest
# docker push featureformcom/dashboard:latest

FROM node:16-alpine
COPY ./dashboard ./dashboard
WORKDIR ./dashboard
RUN npm install --legacy-peer-deps
RUN npm run build
RUN rm -r node_modules


FROM nginx

WORKDIR /app

COPY --from=0 ./dashboard ./dashboard
COPY nginx.conf /etc/nginx/nginx.conf