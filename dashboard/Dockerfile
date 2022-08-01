# RUN docker build . -t dashboard
# docker tag dashboard:latest featureformcom/dashboard:latest
# docker push featureformcom/dashboard:latest

FROM node:16-alpine
COPY ./dashboard ./dashboard
WORKDIR ./dashboard
RUN npm install --production --legacy-peer-deps
RUN npm run build
RUN ls
RUN rm -r node_modules/


FROM node:16-alpine
COPY --from=0 ./dashboard/build ./build


RUN npm uninstall -g serve
RUN npm i -g serve
RUN yarn global add serve
EXPOSE 3000
ENTRYPOINT serve -s build