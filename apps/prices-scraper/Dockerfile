# build stage
FROM node:22.14.0 AS build

RUN corepack enable

RUN apt update && apt-get install -y --no-install-recommends
WORKDIR /usr/src/app

# COPY Workspace config
COPY .yarnrc.yml .
COPY .yarn/ .yarn/
COPY package.json .
COPY yarn.lock .

# COPY service and dependecies packages
COPY apps/prices-scraper ./apps/prices-scraper

RUN yarn workspaces focus prices-scraper 

# production stage
FROM node:22.14.0-bookworm-slim
RUN corepack enable
RUN npm install -g tsx
RUN npm install -g typescript


USER node
WORKDIR /usr/src/app/apps/prices-scraper
COPY --chown=node:node --from=build /usr/src/app/package.json /usr/src/app/package.json
COPY --chown=node:node --from=build /usr/src/app/.yarnrc.yml /usr/src/app/.yarnrc.yml
COPY --chown=node:node --from=build /usr/src/app/yarn.lock /usr/src/app/yarn.lock
COPY --chown=node:node --from=build /usr/src/app/.yarn /usr/src/app/.yarn
COPY --chown=node:node --from=build /usr/src/app/node_modules /usr/src/app/node_modules
COPY --chown=node:node --from=build /usr/src/app/apps/prices-scraper /usr/src/app/apps/prices-scraper

CMD ["yarn","dev"]