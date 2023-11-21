'use client';

export default function Password(props: {value: string | undefined}) {
  return (<>
    <input id='password' type='password' value={props.value} />
    <input type="button" value="Login" onClick={() => {
      fetch('/api/login', {
        method: 'POST',
        body: JSON.stringify({password:(document.getElementById('password') as any).value}),
      });
    }} />
  </>);
}
