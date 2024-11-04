import TimeLabel from '@/components/TimeComponent';
import {
  ListMirrorLogsRequest,
  ListMirrorLogsResponse,
  MirrorLog,
} from '@/grpc_generated/route';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useCallback, useEffect, useState } from 'react';
import 'react-toastify/dist/ReactToastify.css';

const colorForErrorType = (errorType: string) => {
  const errorUpper = errorType.toUpperCase();
  if (errorUpper === 'ERROR') {
    return '#F45156';
  } else if (errorUpper === 'WARNING') {
    return '#FFC107';
  } else {
    return '#4CAF50';
  }
};

const extractFromCloneName = (mirrorOrCloneName: string) => {
  if (mirrorOrCloneName.includes('clone_')) {
    return mirrorOrCloneName.split('_')[1] + ' (initial load)';
  }
  return mirrorOrCloneName;
};

export default function LogsTable({
  numPerPage,
  mirrorName,
  logLevel,
}: {
  numPerPage: number;
  mirrorName: string;
  logLevel: string;
}) {
  const [logs, setLogs] = useState<MirrorLog[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const [[beforeId, afterId], setBeforeAfterId] = useState([-1, -1]);
  const nextPage = useCallback(() => {
    if (logs.length === 0) {
      setBeforeAfterId([-1, -1]);
    }
    setBeforeAfterId([logs[logs.length - 1].id, -1]);
  }, [logs]);
  const prevPage = useCallback(() => {
    if (logs.length === 0 || currentPage < 3) {
      setBeforeAfterId([-1, -1]);
    }
    setBeforeAfterId([-1, logs[0].id]);
  }, [logs, currentPage]);

  useEffect(() => {
    const fetchData = async () => {
      const req: ListMirrorLogsRequest = {
        level: logLevel,
        flowJobName: mirrorName,
        beforeId,
        afterId,
        numPerPage,
        page: 0, // deprecated
      };

      try {
        const response = await fetch('/api/v1/mirrors/logs', {
          method: 'POST',
          cache: 'no-store',
          body: JSON.stringify(req),
        });
        const data: ListMirrorLogsResponse = await response.json();
        const numPages = Math.ceil(data.total / req.numPerPage);
        setLogs(data.errors);
        setTotalPages(numPages);
        setCurrentPage(data.page);
      } catch (error) {
        console.error('Error fetching mirror logs:', error);
      }
    };

    fetchData();
  }, [mirrorName, logLevel, numPerPage, afterId, beforeId]);

  return (
    <Table
      header={
        <TableRow style={{ textAlign: 'left' }}>
          <TableCell>Type</TableCell>
          <TableCell>
            <Label as='label' style={{ fontSize: 15 }}>
              Time
            </Label>
          </TableCell>
          <TableCell>Mirror</TableCell>
          <TableCell>Message</TableCell>
          <TableCell></TableCell>
        </TableRow>
      }
      toolbar={{
        left: (
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <Button variant='normalBorderless' onClick={prevPage}>
              <Icon name='chevron_left' />
            </Button>
            <Button variant='normalBorderless' onClick={nextPage}>
              <Icon name='chevron_right' />
            </Button>
            <Label>{`${currentPage} of ${totalPages}`}</Label>
            <Button
              variant='normalBorderless'
              onClick={() => window.location.reload()}
            >
              <Icon name='refresh' />
            </Button>
          </div>
        ),
      }}
    >
      {logs.map((log, idx) => (
        <TableRow key={`${idx}`}>
          <TableCell
            style={{
              color: colorForErrorType(log.errorType),
              width: '5%',
              fontSize: 14,
            }}
          >
            {log.errorType.toUpperCase()}
          </TableCell>
          <TableCell style={{ width: '10%' }}>
            <TimeLabel fontSize={13} timeVal={new Date(log.errorTimestamp)} />
          </TableCell>
          <TableCell style={{ width: '15%' }}>
            <Label
              as='label'
              style={{ fontSize: 13, width: '90%', overflow: 'auto' }}
            >
              {extractFromCloneName(log.flowName)}
            </Label>
          </TableCell>
          <TableCell style={{ width: '60%', fontSize: 13 }}>
            {log.errorMessage}
          </TableCell>
        </TableRow>
      ))}
    </Table>
  );
}
