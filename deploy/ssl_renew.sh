#!/bin/bash
# Let's Encrypt SSL証明書の自動更新
sudo certbot renew --quiet --nginx
echo "$(date): SSL renewal check completed" >> /home/memestock/ssl_renew.log
