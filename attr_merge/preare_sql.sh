#!/usr/bin/env bash
mysqldump -h10.10.69.170 -ureader -pmiaoji1109 base_data chat_attraction > chat_attraction.sql
mysql -uhourong -phourong attr_merge