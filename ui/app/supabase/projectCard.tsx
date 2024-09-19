'use client';
import { Badge } from '@/lib/Badge';
import { Button } from '@/lib/Button';
import { Label } from '@/lib/Label';
import { ProgressCircle } from '@/lib/ProgressCircle';
import { useRouter } from 'next/navigation';
import { useState } from 'react';
import { SupabaseListProjectsResponse } from '../dto/PeersDTO';
import { ProjectCardStyle, ProjectNameStyle } from './styles';

interface ProjectCardProps {
  project: SupabaseListProjectsResponse;
}

const formatProjectStatus = (status: string) => {
  return status === 'ACTIVE_HEALTHY'
    ? 'Healthy'
    : status === 'INACTIVE'
      ? 'Inactive'
      : 'Unhealthy';
};

const ProjectCard = ({ project }: ProjectCardProps) => {
  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);
  const peerFormLink = `/peers/create/SUPABASE?host=${encodeURIComponent(project.database.host)}&name=${encodeURIComponent(project.name.toLowerCase().replaceAll('-', '_'))}&db=postgres`;
  const goToPeerForm = () => {
    setIsLoading(true);
    router.push(peerFormLink);
  };

  return (
    <Button style={ProjectCardStyle} onClick={goToPeerForm}>
      {isLoading && <ProgressCircle variant='determinate_progress_circle' />}
      <p style={ProjectNameStyle}>{project.name}</p>
      <div
        style={{ display: 'flex', alignItems: 'center', columnGap: '0.5rem' }}
      >
        <div>
          <Label as='label' variant='footnote' colorName='lowContrast'>
            Region: {project.region}
          </Label>
        </div>
        <div>
          <Badge
            variant={
              project.status === 'ACTIVE_HEALTHY'
                ? 'positive'
                : project.status === 'INACTIVE'
                  ? 'normal'
                  : 'destructive'
            }
          >
            <Label
              as='label'
              style={{
                fontSize: 13,
                padding: 0,
              }}
            >
              {formatProjectStatus(project.status)}
            </Label>
          </Badge>
        </div>
      </div>
    </Button>
  );
};

export default ProjectCard;
