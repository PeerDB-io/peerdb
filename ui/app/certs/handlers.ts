import {
  Cert,
  GetCertsResponse,
  PostCertRequest,
} from '@/grpc_generated/route';
import { notifyErr } from '../utils/notify';

export const HandleAddCert = async (cert: Cert) => {
  const addCertRes = await fetch('/api/v1/certs', {
    method: 'POST',
    body: JSON.stringify({
      cert: {
        ...cert,
        id: -1,
      },
    } as PostCertRequest),
  });

  if (!addCertRes.ok) {
    notifyErr('Something went wrong when adding the cert. Please try again');
    return false;
  }
  notifyErr('Successfully added cert', true);
  return true;
};

export const GetCertById = async (certId: string) => {
  try {
    const certByIdRes = await fetch(`/api/v1/certs/${certId}`);
    const certRes: GetCertsResponse = await certByIdRes.json();
    if (!certRes) {
      notifyErr('Cert not found');
      return;
    }
    return certRes.certs.at(0);
  } catch (err) {
    notifyErr(
      'Something went wrong when obtaining the existing cert. Please try again'
    );
    return;
  }
};

export const HandleEditCert = async (cert: Cert) => {
  const editCertRes = await fetch('/api/v1/certs', {
    method: 'POST',
    body: JSON.stringify({
      cert,
    } as PostCertRequest),
  });

  if (!editCertRes.ok) {
    notifyErr('Something went wrong when editing the cert. Please try again');
    return false;
  }
  notifyErr('Successfully edited cert', true);
  return true;
};

export const DeleteCert = async (certId: number) => {
  const deleteCertRes = await fetch(`/api/v1/certs/${certId}`, {
    method: 'DELETE',
  });
  if (!deleteCertRes.ok) {
    notifyErr('Something went wrong when deleting the cert. Please try again');
    return false;
  }

  notifyErr('Successfully deleted cert', true);
  return true;
};
