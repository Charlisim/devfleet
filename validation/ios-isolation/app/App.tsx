import React, {useEffect, useState} from 'react';
import {StyleSheet, Text, View} from 'react-native';
import fixture from './fixture.json';
import revision from './revision';

type Props = {
  nativeEnvironment: string;
  buildId: string;
  backendUrl: string;
  reportUrl: string;
  nonce: string;
};

export default function App(props: Props) {
  const [status, setStatus] = useState('Connecting');
  useEffect(() => {
    let active = true;
    async function report() {
      try {
        const response = await fetch(props.backendUrl);
        if (!response.ok) throw new Error(`Backend HTTP ${response.status}`);
        const backend = await response.json();
        const result = await fetch(props.reportUrl, {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            native_environment: props.nativeEnvironment,
            build_id: props.buildId,
            bundle_environment: fixture.environment,
            backend_environment: backend.environment,
            revision: revision.value,
            nonce: props.nonce,
          }),
        });
        if (!result.ok) throw new Error(`Report HTTP ${result.status}`);
        if (active) setStatus(await result.text());
      } catch (error) {
        if (active) setStatus(String(error));
      }
    }
    void report();
    const timer = setInterval(report, 1000);
    return () => { active = false; clearInterval(timer); };
  }, [props, revision.value]);
  return <View style={styles.screen}>
    <Text style={styles.title}>DevFleet isolation</Text>
    <Text>Native: {props.nativeEnvironment}</Text>
    <Text>Bundle: {fixture.environment}</Text>
    <Text>Revision: {revision.value}</Text>
    <Text>Backend: {props.backendUrl}</Text>
    <Text testID="identity-status">{status}</Text>
  </View>;
}
const styles = StyleSheet.create({
  screen: {flex: 1, justifyContent: 'center', padding: 24, backgroundColor: 'white'},
  title: {fontSize: 26, marginBottom: 20},
});
