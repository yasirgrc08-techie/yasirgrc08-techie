FROM node:22-alpine
WORKDIR /app
COPY package.json server.js ./
COPY server ./server
COPY public ./public
COPY client ./client
ENV NODE_ENV=production \
    PORT=8080 \
    DATA_DIR=/data
RUN mkdir -p /data && chown node:node /data
# Mount a persistent volume here, or uploads won't survive a container restart.
VOLUME ["/data"]
EXPOSE 8080
USER node
CMD ["node", "server.js"]
