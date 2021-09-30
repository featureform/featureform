FROM node:16-alpine
COPY . .
RUN npm install && npm run build && npm install -g serve
EXPOSE 5000
ENTRYPOINT npx serve -s build
