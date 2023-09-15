import { Button } from '@/lib/Button';
import { ButtonGroup } from '@/lib/ButtonGroup';
import { Label } from '@/lib/Label';
import { LayoutMain, RowWithSelect, RowWithTextField } from '@/lib/Layout';
import { Panel } from '@/lib/Panel';
import { Select } from '@/lib/Select';
import { TextField } from '@/lib/TextField';

export default function CreateMirrors() {
  return (
    <LayoutMain width='xxLarge' alignSelf='center' justifySelf='center'>
      <Panel>
        <Label variant='title3' as={'h2'}>
          Create a new mirror
        </Label>
        <Label colorName='lowContrast'>
          Set up a new mirror in a few easy steps.
        </Label>
      </Panel>
      <Panel>
        <Label colorName='lowContrast' variant='subheadline'>
          Details
        </Label>
        <RowWithSelect
          label={
            <Label as='label' htmlFor='mirror'>
              Mirror type
            </Label>
          }
          action={<Select placeholder='Select' id='mirror' />}
        />
        <RowWithTextField
          label={
            <Label as='label' htmlFor='name'>
              Name
            </Label>
          }
          action={
            <TextField placeholder='Placeholder' variant='simple' id='name' />
          }
        />
        <RowWithSelect
          label={
            <Label as='label' htmlFor='source'>
              Source
            </Label>
          }
          action={<Select placeholder='Select' id='source' />}
        />
        <RowWithSelect
          label={
            <Label as='label' htmlFor='destination'>
              Destination
            </Label>
          }
          action={<Select placeholder='Select' id='destination' />}
        />
        <RowWithTextField
          label={
            <Label as='label' htmlFor='query'>
              Query
            </Label>
          }
          action={
            <TextField placeholder='Placeholder' variant='simple' id='query' />
          }
        />
        <RowWithTextField
          label={
            <Label as='label' htmlFor='watermark-id'>
              Watermark ID
            </Label>
          }
          action={
            <TextField
              placeholder='Placeholder'
              variant='simple'
              id='watermark-id'
            />
          }
        />
        <RowWithTextField
          label={
            <Label as='label' htmlFor='watermark-table'>
              Watermark table
            </Label>
          }
          action={
            <TextField
              placeholder='Placeholder'
              variant='simple'
              id='watermark-table'
            />
          }
        />
        <RowWithTextField
          label={
            <Label as='label' htmlFor='partitions'>
              Rows per partition
            </Label>
          }
          action={
            <TextField
              placeholder='Placeholder'
              variant='simple'
              id='partitions'
            />
          }
        />
      </Panel>
      <Panel>
        <ButtonGroup className='justify-end'>
          <Button>Cancel</Button>
          <Button variant='normalSolid'>Continue</Button>
        </ButtonGroup>
      </Panel>
    </LayoutMain>
  );
}
