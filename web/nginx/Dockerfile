# Version: 1.0
FROM nginx:1.21-alpine
RUN rm /etc/nginx/conf.d/default.conf
COPY ./default.conf /etc/nginx/conf.d/
RUN mkdir -p /var/www/web/assets/
RUN mkdir -p /var/www/web/media/
RUN chmod -R 755 /var/www/web/