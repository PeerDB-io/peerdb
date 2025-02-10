import { Label } from '@/lib/Label';
import { Tooltip } from '@/lib/Tooltip';

export function RequiredIndicator(required?: boolean) {
  return (
    required && (
      <Tooltip style={{ width: '100%' }} content={'This is a required field.'}>
        <Label colorName='lowContrast' colorSet='destructive'>
          *
        </Label>
      </Tooltip>
    )
  );
}
