'use client';
import { PeerSetter } from '@/app/dto/PeersDTO';
import { PeerSetting } from '@/app/peers/create/[peerType]/helpers/common';
import { SSHSetting } from '@/app/peers/create/[peerType]/helpers/ssh';
import { SSHConfig } from '@/grpc_generated/peers';
import { Dispatch, SetStateAction } from 'react';

export function handleFile(
  file: File,
  setFile: (value: string, setter: PeerSetter) => void,
  setter: PeerSetter
) {
  if (file) {
    const reader = new FileReader();
    reader.readAsText(file);
    reader.onload = () => {
      setFile(reader.result as string, setter);
    };
    reader.onerror = (error) => {
      console.log(error);
    };
  }
}

export function handleFieldChange(
  e: React.ChangeEvent<HTMLInputElement>,
  setting: PeerSetting,
  setter: PeerSetter
) {
  if (setting.type === 'file') {
    if (e.target.files)
      handleFile(e.target.files[0], setting.stateHandler, setter);
  } else {
    setting.stateHandler(e.target.value, setter);
  }
}

export function handleSSHParam(
  e: React.ChangeEvent<HTMLInputElement>,
  setting: SSHSetting,
  setter: Dispatch<SetStateAction<SSHConfig>>
) {
  if (setting.type === 'file') {
    if (e.target.files) {
      const file = e.target.files[0];
      if (file) {
        const reader = new FileReader();
        reader.readAsText(file);
        reader.onload = () => {
          const fileContents = reader.result as string;
          const base64EncodedContents = Buffer.from(
            fileContents,
            'utf-8'
          ).toString('base64');
          setting.stateHandler(base64EncodedContents, setter);
        };
        reader.onerror = (error) => {
          console.log(error);
        };
      }
    }
  } else {
    setting.stateHandler(e.target.value, setter);
  }
}
