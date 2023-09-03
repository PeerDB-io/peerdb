import { Children, PropsWithChildren } from 'react';
import { Separator } from '../Separator';
import { RenderSlot } from '../types';
import { isDefined } from '../utils/isDefined';
import { renderSlotWith } from '../utils/renderSlotWith';
import {
  StyledTable,
  StyledTableBody,
  StyledTableHeader,
  StyledWrapper,
  TableWrapper,
  ToolbarSlot,
  ToolbarWrapper,
} from './Table.styles';

type TableProps = PropsWithChildren<{
  title?: RenderSlot;
  toolbar?: {
    left?: RenderSlot;
    right?: RenderSlot;
  };
  header?: RenderSlot;
}>;

export function Table({ title, toolbar, header, children }: TableProps) {
  const arrayChildren = Children.toArray(children);

  const Title = isDefined(title) && (
    <>
      {renderSlotWith(title, { variant: 'headline' })}
      <Separator height='thin' variant='empty' />
    </>
  );

  const ToolbarLeft = isDefined(toolbar?.left) && (
    <ToolbarSlot>{renderSlotWith(toolbar?.left)}</ToolbarSlot>
  );
  const ToolbarRight = isDefined(toolbar?.right) && (
    <ToolbarSlot>{renderSlotWith(toolbar?.right)}</ToolbarSlot>
  );

  const Toolbar = isDefined(toolbar) && (
    <>
      <ToolbarWrapper>
        {ToolbarLeft}
        {ToolbarRight}
      </ToolbarWrapper>
      <Separator height='tall' variant='indent' />
    </>
  );

  const Header = isDefined(header) && (
    <StyledTableHeader>{renderSlotWith(header)}</StyledTableHeader>
  );

  return (
    <StyledWrapper>
      {Title}
      {Toolbar}
      <TableWrapper>
        <StyledTable>
          {Header}
          <StyledTableBody>
            {isDefined(header) && (
              <Separator height='thin' variant='indent' as='tr' />
            )}
            {Children.map(arrayChildren, (child, index) => {
              const isLast = index === arrayChildren.length - 1;

              return (
                <>
                  {child}
                  {!isLast && (
                    <Separator height='tall' variant='indent' as='tr' />
                  )}
                </>
              );
            })}
          </StyledTableBody>
        </StyledTable>
      </TableWrapper>
    </StyledWrapper>
  );
}
