FROM node:16-alpine

WORKDIR /blockchain-watcher

COPY ./package.json .

RUN npm install --production

COPY . .

EXPOSE 80

CMD ["node", "./index.js"]
