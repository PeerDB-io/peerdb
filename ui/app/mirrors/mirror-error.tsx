'use client';

import { Icon } from "@/lib/Icon";
import { Prisma } from "@prisma/client";
import React, { useState, useEffect } from "react";
import * as Popover from '@radix-ui/react-popover';
import { ProgressCircle } from "@/lib/ProgressCircle";
import styled, { css } from 'styled-components';


// const NoErrorMirror = styled.div`
// color:  ${({ theme }) => theme.colors.positive.fill.normal};
// `;

// const ErroredMirror = styled.div`
// color:  ${({ theme }) => theme.colors.destructive.fill.normal};
// `;


export const ErrorModal = ({ flowErrors }: { flowErrors: Prisma.flow_errorsSelect[] }) => {
  return (
    <Icon name='error' />
  );
};


export const MirrorError = ({ flowName }: { flowName: string }) => {
  const [flowErrors, setFlowErrors] = useState<Prisma.flow_errorsSelect[]>();
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      setIsLoading(true);
      try {
        const response = await fetch(`/api/mirrors/alerts`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ flowName }),
        });

        if (!response.ok) {
          throw new Error('Network response was not ok');
        }

        const data = await response.json();
        setFlowErrors(data.errors);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [flowName]);

  if (isLoading) {
    return <div><ProgressCircle variant="intermediate_progress_circle" /></div>;
  }

  if (error) {
    console.log(error);
    return <div><Icon name='error' /></div>;
  }

  if (!flowErrors || flowErrors.length === 0) {
    return <Icon name='check_circle' fill={true} />;
  }

  return <ErrorModal flowErrors={flowErrors} />;
};
