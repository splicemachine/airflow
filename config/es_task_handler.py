from datetime import datetime
from typing import List, Optional, Tuple

import pendulum
from elasticsearch_dsl import Search

from airflow.models import TaskInstance
from airflow.utils import timezone
from airflow.providers.elasticsearch.log.es_task_handler import ElasticsearchTaskHandler

EsLogMsgType = List[Tuple[str, str]]

class ESTaskHandler(ElasticsearchTaskHandler):
    @staticmethod
    def _clean_execution_date(execution_date: datetime) -> str:
        """
        Clean up an execution date so that it is safe to query in elasticsearch
        by removing reserved characters.
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters

        :param execution_date: execution date of the dag run.
        """
        return execution_date.isoformat().replace(":", "_").replace('+', '_plus_')

    def _read(
        self, ti: TaskInstance, try_number: int, metadata: Optional[dict] = None
    ) -> Tuple[EsLogMsgType, dict]:
        """
        Endpoint for streaming log.

        :param ti: task instance object
        :param try_number: try_number of the task instance
        :param metadata: log metadata,
                         can be used for steaming log reading and auto-tailing.
        :return: a list of tuple with host and log documents, metadata.
        """
        if not metadata:
            metadata = {'offset': 0}
        if 'offset' not in metadata:
            metadata['offset'] = 0

        offset = metadata['offset']
        log_id = self._render_log_id(ti, try_number)

        logs = self.es_read(log_id, offset, metadata)
        logs_by_host = self._group_logs_by_host(logs)

        next_offset = offset if not logs else logs[-1].log.offset

        # Ensure a string here. Large offset numbers will get JSON.parsed incorrectly
        # on the client. Sending as a string prevents this issue.
        # https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/MAX_SAFE_INTEGER
        metadata['offset'] = str(next_offset)

        # end_of_log_mark may contain characters like '\n' which is needed to
        # have the log uploaded but will not be stored in elasticsearch.
        loading_hosts = [
            item[0] for item in logs_by_host if item[-1][-1].message != self.end_of_log_mark.strip()
        ]
        metadata['end_of_log'] = False if not logs else len(loading_hosts) == 0

        cur_ts = pendulum.now()
        # Assume end of log after not receiving new log for 5 min,
        # as executor heartbeat is 1 min and there might be some
        # delay before Elasticsearch makes the log available.
        if 'last_log_timestamp' in metadata:
            last_log_ts = timezone.parse(metadata['last_log_timestamp'])
            if (
                cur_ts.diff(last_log_ts).in_minutes() >= 5
                or 'max_offset' in metadata
                and int(offset) >= int(metadata['max_offset'])
            ):
                metadata['end_of_log'] = True

        if int(offset) != int(next_offset) or 'last_log_timestamp' not in metadata:
            metadata['last_log_timestamp'] = str(cur_ts)

        # If we hit the end of the log, remove the actual end_of_log message
        # to prevent it from showing in the UI.
        def concat_logs(lines):
            log_range = (len(lines) - 1) if lines[-1].message == self.end_of_log_mark.strip() else len(lines)
            return '\n'.join([self._format_msg(lines[i]) for i in range(log_range)])

        message = [(host, concat_logs(hosted_log)) for host, hosted_log in logs_by_host]

        return message, metadata
    
    def es_read(self, log_id: str, offset: str, metadata: dict) -> list:
        """
        Returns the logs matching log_id in Elasticsearch and next offset.
        Returns '' if no log is found or there was an error.

        :param log_id: the log_id of the log to read.
        :type log_id: str
        :param offset: the offset start to read log from.
        :type offset: str
        :param metadata: log metadata, used for steaming log download.
        :type metadata: dict
        """
        # Offset is the unique key for sorting logs given log_id.
        search = Search(using=self.client).query('match_phrase', log_id=log_id).sort('log.offset')

        search = search.filter('range', **{'log.offset': {'gt': int(offset)}})
        max_log_line = search.count()
        if 'download_logs' in metadata and metadata['download_logs'] and 'max_offset' not in metadata:
            try:
                if max_log_line > 0:
                    metadata['max_offset'] = search[max_log_line - 1].execute()[-1].log.offset
                else:
                    metadata['max_offset'] = 0
            except Exception:  # pylint: disable=broad-except
                self.log.exception('Could not get current log size with log_id: %s', log_id)

        logs = []
        if max_log_line != 0:
            try:

                logs = search[self.MAX_LINE_PER_PAGE * self.PAGE : self.MAX_LINE_PER_PAGE].execute()
            except Exception as e:  # pylint: disable=broad-except
                self.log.exception('Could not read log with log_id: %s, error: %s', log_id, str(e))

        return logs
