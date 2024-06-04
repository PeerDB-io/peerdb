import { MirrorLog } from '@/app/dto/AlertDTO';
import TimeLabel from '@/components/TimeComponent';
import { Button } from '@/lib/Button';
import { Icon } from '@/lib/Icon';
import { Label } from '@/lib/Label';
import { Table, TableCell, TableRow } from '@/lib/Table';
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

const LogsTable = ({
  logs,
  currentPage,
  totalPages,
  setCurrentPage,
}: {
  logs: MirrorLog[];
  currentPage: number;
  totalPages: number;
  setCurrentPage: (page: number) => void;
}) => {
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
      {logs.map((log, idx) => (
        <TableRow key={`${currentPage}_${idx}`}>
          <TableCell
            style={{
              color: colorForErrorType(log.error_type),
              width: '5%',
              fontSize: 14,
            }}
          >
            {log.error_type.toUpperCase()}
          </TableCell>
          <TableCell style={{ width: '10%' }}>
            <TimeLabel fontSize={13} timeVal={log.error_timestamp} />
          </TableCell>
          <TableCell style={{ width: '15%' }}>
            <Label
              as='label'
              style={{ fontSize: 13, width: '90%', overflow: 'auto' }}
            >
              {extractFromCloneName(log.flow_name)}
            </Label>
          </TableCell>
          <TableCell style={{ width: '60%', fontSize: 13 }}>
            {log.error_message}
          </TableCell>
        </TableRow>
      ))}
    </Table>
  );
};

export default LogsTable;
