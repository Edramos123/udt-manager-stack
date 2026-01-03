FROM node:20-alpine
WORKDIR /app
COPY server.js .
RUN npm init -y && npm i express mongodb
EXPOSE 8080
CMD ["node", "server.js"]
