FROM openresty/openresty:1.25.3.1-0-alpine

RUN apk add --no-cache perl bash ca-certificates curl \
  && update-ca-certificates \
  && /usr/local/openresty/bin/opm get ledgetech/lua-resty-http

RUN mkdir -p /usr/local/openresty/nginx/conf/conf.d \
    && mkdir -p /usr/local/openresty/nginx/lua

COPY nginx.conf /usr/local/openresty/nginx/conf/nginx.conf
COPY conf.d/ /usr/local/openresty/nginx/conf/conf.d/
COPY lua/ /usr/local/openresty/nginx/lua/

EXPOSE 80
CMD ["/usr/local/openresty/bin/openresty", "-g", "daemon off;"]
