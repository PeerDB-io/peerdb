'use client';

import { Children, PropsWithChildren } from 'react';
import { Separator } from '../Separator';
import { RenderObject } from '../types';
import { isDefined } from '../utils/isDefined';
import { renderObjectWith } from '../utils/renderObjectWith';
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
  title?: RenderObject;
  toolbar?: {
    left?: RenderObject;
    right?: RenderObject;
  };
  header?: RenderObject;
}>;

export function Table({ title, toolbar, header, children }: TableProps) {
  const arrayChildren = Children.toArray(children);

  const Title = isDefined(title) && (
    <>
      {renderObjectWith(title, { variant: 'headline' })}
      <Separator height='thin' variant='empty' />
    </>
  );

  const ToolbarLeft = isDefined(toolbar?.left) && (
    <ToolbarSlot>{renderObjectWith(toolbar?.left)}</ToolbarSlot>
  );
  const ToolbarRight = isDefined(toolbar?.right) && (
    <ToolbarSlot>{renderObjectWith(toolbar?.right)}</ToolbarSlot>
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
    <StyledTableHeader>{renderObjectWith(header)}</StyledTableHeader>
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
