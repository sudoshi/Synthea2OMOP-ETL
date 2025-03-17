FROM node:16-alpine

WORKDIR /app

COPY package.json ./
RUN npm install && \
    mkdir -p /app/node_modules/.cache && \
    chmod -R 777 /app/node_modules/.cache

COPY . .

EXPOSE 3000

CMD ["npm", "start"]
