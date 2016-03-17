/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.dataflow.std.sample;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Array;
import java.util.Arrays;

import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.junit.Test;

public class TypeTranslateTest {

    @Test
    public void testIntPoitable() throws HyracksException {
        //IBinaryComparator comp = PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY).createBinaryComparator();
        byte[] ip255 = new byte[4];
        ip255[3] |= 0xff;
        IPointable ip = new IntegerPointable();
        ip.set(ip255, 0, IntegerPointable.TYPE_TRAITS.getFixedLength());
        int iip = ((IntegerPointable) ip).getInteger();
        assertEquals(iip, 255);
        ip255[2] |= 0xff;
        ip.set(ip255, 0, IntegerPointable.TYPE_TRAITS.getFixedLength());
        iip = ((IntegerPointable) ip).getInteger();
        assertEquals(iip, 65535);

        char[] sc = new String("2N|5n,3+").toCharArray();
        IPointable sp = new UTF8StringPointable();
        byte[] bsc = SampleUtils.ansiToUTF8Byte(sc, 0);
        sp.set(bsc, 0, bsc.length);
        Arrays.fill(sc, '\0');
        StringBuilder sb = new StringBuilder();
        UTF8StringPointable.toString(sb, sp.getByteArray(), 0);
        String s1 = sb.toString();
        String s2 = "2N|5n,3+";
        assertEquals(s1, s2);

        char[] usc = new String("横空出世").toCharArray();
        StringBuilder ucb = new StringBuilder();
        ucb.append(usc);
        IPointable usp = new UTF8StringPointable();
        byte[] ubsc = SampleUtils.toUTF8Byte(usc, 0);
        usp.set(ubsc, 0, ubsc.length);
        Arrays.fill(usc, '\0');
        StringBuilder usb = new StringBuilder();
        UTF8StringPointable.toString(usb, usp.getByteArray(), 0);
        String us1 = usb.toString();
        String us2 = "横空出世";
        assertEquals(us1, us2);

        long quantile = SampleUtils.ansiMappingToLong((UTF8StringPointable) sp, 0, 8);
        UTF8StringPointable sQuan = SampleUtils.longMappingToAnsi(quantile, 8);
        StringBuilder sb1 = new StringBuilder();
        UTF8StringPointable.toString(sb1, sQuan.getByteArray(), 0);
        assertEquals(sb.toString(), sb1.toString());
        return;
    }
}
