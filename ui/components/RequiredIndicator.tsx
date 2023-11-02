import { Label } from '@/lib/Label';
import { Tooltip } from '@/lib/Tooltip';

export const RequiredIndicator = (required?: boolean) => {
  if (required)
    return (
      <Tooltip style={{ width: '100%' }} content={'This is a required field.'}>
        <Label colorName='lowContrast' colorSet='destructive'>
          *
        </Label>
      </Tooltip>
    );
  return <></>;
};
