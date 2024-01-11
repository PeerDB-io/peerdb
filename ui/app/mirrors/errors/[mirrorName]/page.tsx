'use client';

import {
  MirrorLog,
  MirrorLogsRequest,
  MirrorLogsResponse,
} from '@/app/dto/AlertDTO';
import TimeLabel from '@/components/TimeComponent';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
import { useParams } from 'next/navigation';
import { useEffect, useState } from 'react';
import { ToastContainer } from 'react-toastify';
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

export default function MirrorError() {
  const params = useParams<{ mirrorName: string }>();
  const [mirrorErrors, setMirrorErrors] = useState<MirrorLog[]>([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);

  useEffect(() => {
    setCurrentPage(1);
  }, [params.mirrorName]);

  const req: MirrorLogsRequest = {
    flowJobName: params.mirrorName,
    page: currentPage,
    numPerPage: 10,
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch('/api/mirrors/errors', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(req),
        });
        const data: MirrorLogsResponse = await response.json();
        const numPages = Math.ceil(data.total / req.numPerPage);
        setMirrorErrors(data.errors);
        setTotalPages(numPages);
      } catch (error) {
        console.error('Error fetching mirror errors:', error);
      }
    };

    fetchData();
  }, [currentPage, params.mirrorName]);

  const handleNextPage = () => {
    if (currentPage < totalPages) {
      setCurrentPage(currentPage + 1);
    }
  };

  const handlePrevPage = () => {
    if (currentPage > 1) {
      setCurrentPage(currentPage - 1);
    }
  };

  return (
    <>
      <div style={{ padding: '2rem' }}>
        <Label variant='title2'>Logs</Label>
        <hr></hr>
        <div style={{ marginTop: '1rem' }}>
          <Label variant='body'>
            <b>Mirror name</b>:
          </Label>
          <Label variant='body'>{params.mirrorName}</Label>

          <div>
            <Label as='label' style={{ fontSize: 14, marginTop: '1rem' }}>
              Here you can view logs for your mirror.
            </Label>
          </div>

          <Table
            header={
              <TableRow style={{ textAlign: 'left' }}>
                <TableCell>Type</TableCell>
                <TableCell>
                  <Label as='label' style={{ fontSize: 15 }}>
                    Time
                  </Label>
                </TableCell>
                <TableCell>Message</TableCell>
                <TableCell></TableCell>
              </TableRow>
            }
            toolbar={{
              left: (
                <div style={{ display: 'flex', alignItems: 'center' }}>
                  <Button variant='normalBorderless' onClick={handlePrevPage}>
                    <Icon name='chevron_left' />
                  </Button>
                  <Button variant='normalBorderless' onClick={handleNextPage}>
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
            {mirrorErrors.map((mirrorError, idx) => (
              <TableRow key={`${currentPage}_${idx}`}>
                <TableCell
                  style={{
                    color: colorForErrorType(mirrorError.error_type),
                    width: '10%',
                  }}
                >
                  {mirrorError.error_type.toUpperCase()}
                </TableCell>
                <TableCell style={{ width: '20%' }}>
                  <TimeLabel
                    fontSize={14}
                    timeVal={mirrorError.error_timestamp}
                  />
                </TableCell>
                <TableCell style={{ width: '50%', fontSize: 13 }}>
                  {mirrorError.error_message}
                </TableCell>
              </TableRow>
            ))}
          </Table>
        </div>
      </div>
      <ToastContainer />
    </>
  );
}
