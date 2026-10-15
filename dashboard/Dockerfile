FROM node:22-alpine
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts --no-audit --no-fund
COPY sever.js monitor.js metrics.js ./
COPY public/ ./public/
USER node
EXPOSE 8090
CMD ["npm", "start"]
